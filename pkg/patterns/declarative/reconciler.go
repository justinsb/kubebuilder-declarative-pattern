/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package declarative

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	recorder "k8s.io/client-go/tools/record"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative/pkg/applier"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative/pkg/manifest"
	"sigs.k8s.io/kustomize/api/filesys"
	"sigs.k8s.io/kustomize/api/krusty"
)

var _ reconcile.Reconciler = &Reconciler{}

type Reconciler struct {
	prototype DeclarativeObject
	client    client.Client
	config    *rest.Config
	kubectl   Applyer

	rm  reconcileMetrics
	mgr manager.Manager

	// recorder is the EventRecorder for creating k8s events
	recorder      recorder.EventRecorder
	dynamicClient dynamic.Interface

	restMapper meta.RESTMapper
	options    reconcilerParams
}

type Applyer interface {
	Apply(ctx context.Context, namespace string, objects *manifest.Objects, validate bool, args ...string) error
}

type DeclarativeObject interface {
	runtime.Object
	metav1.Object
}

// For mocking
var kubectl = applier.NewDirectApplier()

func (r *Reconciler) Init(mgr manager.Manager, prototype DeclarativeObject, opts ...reconcilerOption) error {
	r.prototype = prototype
	r.kubectl = kubectl

	// TODO: Can we derive the name from prototype?
	controllerName := "addon-controller"
	r.recorder = mgr.GetEventRecorderFor(controllerName)

	r.client = mgr.GetClient()
	r.config = mgr.GetConfig()
	r.mgr = mgr
	globalObjectTracker.mgr = mgr

	d, err := dynamic.NewForConfig(r.config)
	if err != nil {
		return err
	}
	r.dynamicClient = d

	restMapper, err := apiutil.NewDiscoveryRESTMapper(r.config)
	if err != nil {
		return err
	}
	r.restMapper = restMapper

	if err = r.applyOptions(opts...); err != nil {
		return err
	}

	if err := r.validateOptions(); err != nil {
		return err
	}

	if r.CollectMetrics() {
		if gvk, err := apiutil.GVKForObject(prototype, r.mgr.GetScheme()); err != nil {
			return err
		} else {
			reconcileMetricsFor(gvk)
		}
	}

	return nil
}

// +rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (result reconcile.Result, err error) {
	log := log.Log
	defer r.collectMetrics(request, result, err)

	// Fetch our CRD instance
	instance := r.prototype.DeepCopyObject().(DeclarativeObject)
	if err = r.client.Get(ctx, request.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "error reading object")
		return reconcile.Result{}, err
	}

	if r.options.status != nil {
		if err := r.options.status.Preflight(ctx, instance); err != nil {
			log.Error(err, "preflight check failed, not reconciling")
			return reconcile.Result{}, err
		}
	}

	return r.reconcileExists(ctx, request.NamespacedName, instance)
}

func (r *Reconciler) reconcileExists(ctx context.Context, name types.NamespacedName, instance DeclarativeObject) (reconcile.Result, error) {
	log := log.Log
	log.WithValues("object", name.String()).Info("reconciling")

	var fs filesys.FileSystem
	if r.IsKustomizeOptionUsed() {
		fs = filesys.MakeFsInMemory()
	}

	objects, err := r.BuildDeploymentObjectsWithFs(ctx, name, instance, fs)
	if err != nil {
		log.Error(err, "building deployment objects")
		return reconcile.Result{}, fmt.Errorf("error building deployment objects: %v", err)
	}
	log.WithValues("objects", fmt.Sprintf("%d", len(objects.Items))).Info("built deployment objects")

	if r.options.status != nil {
		isValidVersion, err := r.options.status.VersionCheck(ctx, instance, objects)
		if err != nil {
			if !isValidVersion {
				// r.client isn't exported so can't be updated in version check function
				if err := r.client.Status().Update(ctx, instance); err != nil {
					return reconcile.Result{}, err
				}
				r.recorder.Event(instance, "Warning", "Failed version check", err.Error())
				log.Error(err, "Version check failed, not reconciling")
				return reconcile.Result{}, nil
			}
			log.Error(err, "Version check failed, trying to reconcile")
			return reconcile.Result{}, err
		}
	}

	defer func() {
		if r.options.status != nil {
			if err := r.options.status.Reconciled(ctx, instance, objects); err != nil {
				log.Error(err, "failed to reconcile status")
			}
		}
	}()

	objects, err = parseListKind(objects)

	if err != nil {
		log.Error(err, "Parsing list kind")
		return reconcile.Result{}, fmt.Errorf("error parsing list kind: %v", err)
	}

	err = r.injectOwnerRef(ctx, instance, objects)
	if err != nil {
		return reconcile.Result{}, err
	}

	var newItems []*manifest.Object
	for _, obj := range objects.Items {

		unstruct, err := GetObjectFromCluster(obj, r)
		if err != nil && !apierrors.IsNotFound(err) {
			log.WithValues("name", obj.Name).Error(err, "Unable to get resource")
		}
		if unstruct != nil {
			annotations := unstruct.GetAnnotations()
			if _, ok := annotations["addons.k8s.io/ignore"]; ok {
				log.WithValues("kind", obj.Kind).WithValues("name", obj.Name).Info("Found ignore annotation on object, " +
					"skipping object")
				continue
			}
		}
		newItems = append(newItems, obj)
	}
	objects.Items = newItems

	extraArgs := []string{"--force"}

	if r.options.prune {
		var labels []string
		for k, v := range r.options.labelMaker(ctx, instance) {
			labels = append(labels, fmt.Sprintf("%s=%s", k, v))
		}

		extraArgs = append(extraArgs, "--prune", "--selector", strings.Join(labels, ","))
	}

	ns := ""
	if !r.options.preserveNamespace {
		ns = name.Namespace
	}

	if r.CollectMetrics() {
		if errs := globalObjectTracker.addIfNotPresent(objects.Items, ns); errs != nil {
			for _, err := range errs.Errors() {
				if errors.Is(err, noRESTMapperErr{}) {
					log.WithName("declarative_reconciler").Error(err, "failed to get corresponding RESTMapper from API server")
				} else if errors.Is(err, emptyNamespaceErr{}) {
					// There should be no route to this path
					log.WithName("declarative_reconciler").Info("Scoped object, but no namespace specified")
				} else {
					log.WithName("declarative_reconciler").Error(err, "Unknown error")
				}
			}
		}
	}

	kubectl := r.kubectl
	if r.options.overrideTargetCluster != nil {
		if k, err := r.options.overrideTargetCluster(ctx, instance); err != nil {
			log.Error(err, "error while overriding target cluster")
			return reconcile.Result{}, err
		} else if k != nil {
			kubectl = k
		}
	}

	if err := kubectl.Apply(ctx, ns, objects, r.options.validate, extraArgs...); err != nil {
		log.Error(err, "applying manifest")
		return reconcile.Result{}, fmt.Errorf("error applying manifest: %v", err)
	}

	if r.options.sink != nil {
		if err := r.options.sink.Notify(ctx, instance, objects); err != nil {
			log.Error(err, "notifying sink")
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

// BuildDeploymentObjects performs all manifest operations to build a final set of objects for deployment
func (r *Reconciler) BuildDeploymentObjects(ctx context.Context, name types.NamespacedName, instance DeclarativeObject) (*manifest.Objects, error) {
	return r.BuildDeploymentObjectsWithFs(ctx, name, instance, nil)
}

// BuildDeploymentObjectsWithFs is the implementation of BuildDeploymentObjects, supporting saving to a filesystem for kustomize
// If fs is provided, the transformed manifests will be saved to that filesystem
func (r *Reconciler) BuildDeploymentObjectsWithFs(ctx context.Context, name types.NamespacedName, instance DeclarativeObject, fs filesys.FileSystem) (*manifest.Objects, error) {
	log := log.Log

	// 1. Load the manifest
	manifestFiles, err := r.loadRawManifest(ctx, instance)
	if err != nil {
		log.Error(err, "error loading raw manifest")
		return nil, err
	}
	manifestObjects := &manifest.Objects{}
	// 2. Perform raw string operations
	for manifestPath, manifestStr := range manifestFiles {
		for _, t := range r.options.rawManifestOperations {
			transformed, err := t(ctx, instance, manifestStr)
			if err != nil {
				log.Error(err, "error performing raw manifest operations")
				return nil, err
			}
			manifestStr = transformed
		}

		// 3. Parse manifest into objects
		// 4. Perform object transformations
		objects, err := r.parseAndTransformManifest(ctx, instance, manifestStr)
		if err != nil {
			log.Error(err, "error parsing and transforming manifest")
			return nil, err
		}

		if fs != nil {
			// 5. Write objects to filesystem for kustomizing
			for _, item := range objects.Items {
				json, err := item.JSON()
				if err != nil {
					log.Error(err, "error converting object to json")
					return nil, err
				}
				fs.WriteFile(string(manifestPath), json)
			}
			for _, blob := range objects.Blobs {
				fs.WriteFile(string(manifestPath), blob)
			}
		}
		manifestObjects.Path = filepath.Dir(manifestPath)
		manifestObjects.Items = append(manifestObjects.Items, objects.Items...)
		manifestObjects.Blobs = append(manifestObjects.Blobs, objects.Blobs...)
	}

	// If Kustomize option is on, it's assumed that the entire addon manifest is created using Kustomize
	// Here, the manifest is built using Kustomize and then replaces the Object items with the created manifest
	if r.IsKustomizeOptionUsed() {
		// run kustomize to create final manifest
		opts := krusty.MakeDefaultOptions()
		k := krusty.MakeKustomizer(fs, opts)
		m, err := k.Run(manifestObjects.Path)
		if err != nil {
			log.Error(err, "running kustomize to create final manifest")
			return nil, fmt.Errorf("error running kustomize: %v", err)
		}

		manifestYaml, err := m.AsYaml()
		if err != nil {
			log.Error(err, "creating final manifest yaml")
			return nil, fmt.Errorf("error converting kustomize output to yaml: %v", err)
		}

		objects, err := r.parseAndTransformManifest(ctx, instance, string(manifestYaml))
		if err != nil {
			log.Error(err, "creating final manifest yaml")
			return nil, err
		}
		manifestObjects.Items = objects.Items
	}

	// 6. Sort objects to work around dependent objects in the same manifest (eg: service-account, deployment)
	manifestObjects.Sort(DefaultObjectOrder(ctx))

	return manifestObjects, nil
}

// parseAndTransformManifest parses the manifest into objects and adds any transformations as required
func (r *Reconciler) parseAndTransformManifest(ctx context.Context, instance DeclarativeObject, manifestStr string) (*manifest.Objects, error) {
	log := log.Log

	objects, err := manifest.ParseObjects(ctx, manifestStr)
	if err != nil {
		log.Error(err, "error parsing manifest")
		return nil, err
	}

	transforms := r.options.objectTransformations
	if r.options.labelMaker != nil {
		transforms = append(transforms, AddLabels(r.options.labelMaker(ctx, instance)))
	}
	// TODO(jrjohnson): apply namespace here
	for _, t := range transforms {
		err := t(ctx, instance, objects)
		if err != nil {
			return nil, err
		}
	}
	return objects, nil
}

// loadRawManifest loads the raw manifest YAML from the repository
func (r *Reconciler) loadRawManifest(ctx context.Context, o DeclarativeObject) (map[string]string, error) {
	s, err := r.options.manifestController.ResolveManifest(ctx, o)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (r *Reconciler) applyOptions(opts ...reconcilerOption) error {
	params := reconcilerParams{}

	opts = append(Options.Begin, opts...)
	opts = append(opts, Options.End...)

	for _, opt := range opts {
		params = opt(params)
	}

	// Default the manifest controller if not set
	if params.manifestController == nil && DefaultManifestLoader != nil {
		loader, err := DefaultManifestLoader()
		if err != nil {
			return err
		}
		params.manifestController = loader
	}

	r.options = params
	return nil
}

// Validate compatibility of selected options
func (r *Reconciler) validateOptions() error {
	var errs []string

	if r.options.prune && r.options.labelMaker == nil {
		errs = append(errs, "WithApplyPrune must be used with the WithLabels option")
	}

	if r.options.manifestController == nil {
		errs = append(errs, "ManifestController must be set either by configuring DefaultManifestLoader or specifying the WithManifestController option")
	}

	if len(errs) != 0 {
		return fmt.Errorf(strings.Join(errs, ","))
	}

	return nil
}

func (r *Reconciler) injectOwnerRef(ctx context.Context, instance DeclarativeObject, objects *manifest.Objects) error {
	if r.options.ownerFn == nil {
		return nil
	}

	log := log.Log
	log.WithValues("object", fmt.Sprintf("%s/%s", instance.GetName(), instance.GetNamespace())).Info("injecting owner references")

	for _, o := range objects.Items {
		owner, err := r.options.ownerFn(ctx, instance, *o, *objects)
		if err != nil {
			log.WithValues("object", o).Error(err, "resolving owner ref", o)
			return err
		}
		if owner == nil {
			log.WithValues("object", o).Info("no owner resolved")
			continue
		}
		if owner.GetName() == "" {
			log.WithValues("object", o).Info("has no name")
			continue
		}
		if string(owner.GetUID()) == "" {
			log.WithValues("object", o).Info("has no UID")
			continue
		}

		gvk, err := apiutil.GVKForObject(owner, r.mgr.GetScheme())
		if gvk.Group == "" || gvk.Version == "" {
			log.WithValues("object", o).WithValues("GroupVersionKind", gvk).Info("is not valid")
			continue
		}

		// TODO, error/skip if:
		// - owner is namespaced and o is not
		// - owner is in a different namespace than o

		ownerRefs := []interface{}{
			map[string]interface{}{
				"apiVersion":         gvk.Group + "/" + gvk.Version,
				"blockOwnerDeletion": true,
				"controller":         true,
				"kind":               gvk.Kind,
				"name":               owner.GetName(),
				"uid":                string(owner.GetUID()),
			},
		}
		if err := o.SetNestedField(ownerRefs, "metadata", "ownerReferences"); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) collectMetrics(request reconcile.Request, result reconcile.Result, err error) {
	if r.options.metrics {
		r.rm.reconcileWith(request)
		r.rm.reconcileFailedWith(request, result, err)
	}
}

// IsKustomizeOptionUsed checks if the option for Kustomize build is used for creating manifests
func (r *Reconciler) IsKustomizeOptionUsed() bool {
	if r.options.kustomize {
		return true
	}
	return false
}

// SetSink provides a Sink that will be notified for all deployments
func (r *Reconciler) SetSink(sink Sink) {
	r.options.sink = sink
}

func parseListKind(infos *manifest.Objects) (*manifest.Objects, error) {
	var out []*manifest.Object

	for _, item := range infos.Items {
		if item.Group == "v1" && item.Kind == "List" {
			itemObj := item.UnstructuredObject()

			err := itemObj.EachListItem(func(obj runtime.Object) error {
				itemUnstructured := obj.(*unstructured.Unstructured)
				newObj, err := manifest.NewObject(itemUnstructured)
				if err != nil {
					return err
				}
				out = append(out, newObj)
				return nil
			})

			if err != nil {
				return nil, err
			}
		} else {
			out = append(out, item)
		}
	}

	ret := manifest.Objects{
		Items: out,
		Blobs: infos.Blobs,
		Path:  infos.Path,
	}

	return &ret, nil
}

// CollectMetrics determines whether metrics of declarative reconciler is enabled
func (r *Reconciler) CollectMetrics() bool {
	return r.options.metrics
}

func aggregateStatus(m map[status.Status]bool) status.Status {
	inProgress := m[status.InProgressStatus]
	terminating := m[status.TerminatingStatus]

	failed := m[status.FailedStatus]

	if inProgress || terminating {
		return status.InProgressStatus
	}

	if failed {
		return status.FailedStatus
	}

	return status.CurrentStatus
}

func GetObjectFromCluster(obj *manifest.Object, r *Reconciler) (*unstructured.
	Unstructured, error) {
	getOptions := metav1.GetOptions{}
	gvk := obj.GroupVersionKind()

	mapping, err := r.restMapper.RESTMapping(obj.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("unable to get resource: %v", err)
	}
	ns := obj.UnstructuredObject().GetNamespace()
	unstruct, err := r.dynamicClient.Resource(mapping.Resource).Namespace(ns).Get(context.Background(),
		obj.Name, getOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to get mapping for resource: %v", err)
	}
	return unstruct, nil
}
