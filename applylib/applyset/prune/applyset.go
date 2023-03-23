/*
Copyright 2023 The Kubernetes Authors.

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

package prune

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

// Label and annotation keys from the ApplySet specification.
// https://git.k8s.io/enhancements/keps/sig-cli/3659-kubectl-apply-prune#design-details-applyset-specification
const (
	// ApplySetToolingAnnotation is the key of the label that indicates which tool is used to manage this ApplySet.
	// Tooling should refuse to mutate ApplySets belonging to other tools.
	// The value must be in the format <toolname>/<semver>.
	// Example value: "kubectl/v1.27" or "helm/v3" or "kpt/v1.0.0"
	ApplySetToolingAnnotation = "applyset.kubernetes.io/tooling"

	// ApplySetAdditionalNamespacesAnnotation annotation extends the scope of the ApplySet beyond the parent
	// object's own namespace (if any) to include the listed namespaces. The value is a comma-separated
	// list of the names of namespaces other than the parent's namespace in which objects are found
	// Example value: "kube-system,ns1,ns2".
	ApplySetAdditionalNamespacesAnnotation = "applyset.kubernetes.io/additional-namespaces"

	// ApplySetGRsAnnotation is a list of group-resources used to optimize listing of ApplySet member objects.
	// It is optional in the ApplySet specification, as tools can perform discovery or use a different optimization.
	// However, it is currently required in kubectl.
	// When present, the value of this annotation must be a comma separated list of the group-kinds,
	// in the fully-qualified name format, i.e. <resourcename>.<group>.
	// Example value: "certificates.cert-manager.io,configmaps,deployments.apps,secrets,services"
	ApplySetGRsAnnotation = "applyset.kubernetes.io/contains-group-resources"

	// ApplySetParentIDLabel is the key of the label that makes object an ApplySet parent object.
	// Its value MUST use the format specified in V1ApplySetIdFormat below
	ApplySetParentIDLabel = "applyset.kubernetes.io/id"

	// V1ApplySetIdFormat is the format required for the value of ApplySetParentIDLabel (and ApplysetPartOfLabel).
	// The %s segment is the unique ID of the object itself, which MUST be the base64 encoding
	// (using the URL safe encoding of RFC4648) of the hash of the GKNN of the object it is on, in the form:
	// base64(sha256(<name>.<namespace>.<kind>.<group>)).
	V1ApplySetIdFormat = "applyset-%s-v1"

	// ApplysetPartOfLabel is the key of the label which indicates that the object is a member of an ApplySet.
	// The value of the label MUST match the value of ApplySetParentIDLabel on the parent object.
	ApplysetPartOfLabel = "applyset.kubernetes.io/part-of"

	// ApplysetParentCRDLabel is the key of the label that can be set on a CRD to identify
	// the custom resource type it defines (not the CRD itself) as an allowed parent for an ApplySet.
	ApplysetParentCRDLabel = "applyset.kubernetes.io/is-parent-type"
)

var defaultApplySetParentGVR = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

type ParentObjectRef interface {
	GroupVersionKind() schema.GroupVersionKind
	GetName() string
	GetNamespace() string
}

type ParentObject interface {
	ParentObjectRef

	GetLabels() map[string]string
	GetAnnotations() map[string]string

	Patch(ctx context.Context, patchType types.PatchType, patch []byte, options metav1.PatchOptions) error
}

// ApplySet tracks the information about an applyset apply/prune
type ApplySet struct {
	// parentRef is a reference to the parent object that is used to track the applyset.
	// parentRef ParentObjectRef
	id string

	// toolingID is the value to be used and validated in the applyset.kubernetes.io/tooling annotation.
	toolingID ApplySetTooling

	// currentResources is the set of resources that are part of the sever-side set as of when the current operation started.
	currentResources map[schema.GroupKind]bool

	// currentNamespaces is the set of namespaces that contain objects in this applyset as of when the current operation started.
	currentNamespaces sets.Set[string]

	// updatedResources is the set of resources that will be part of the set as of when the current operation completes.
	updatedResources map[schema.GroupKind]bool

	// updatedNamespaces is the set of namespaces that will contain objects in this applyset as of when the current operation completes.
	updatedNamespaces sets.Set[string]

	restMapper meta.RESTMapper

	// client is a client specific to the ApplySet parent object's type
	client dynamic.Interface

	visitedUids sets.Set[types.UID]
}

var builtinApplySetParentGVRs = sets.New[schema.GroupVersionResource](
	defaultApplySetParentGVR,
	schema.GroupVersionResource{Version: "v1", Resource: "configmaps"},
)

// // ApplySetParentRef stores object and type meta for the parent object that is used to track the applyset.
// type ApplySetParentRef struct {
// 	Name      string
// 	Namespace string
// 	*meta.RESTMapping
// }

// func (p ApplySetParentRef) IsNamespaced() bool {
// 	return p.Scope.Name() == meta.RESTScopeNameNamespace
// }

// // String returns the string representation of the parent object using the same format
// // that we expect to receive in the --applyset flag on the CLI.
// func (p ApplySetParentRef) String() string {
// 	return fmt.Sprintf("%s.%s/%s", p.Resource.Resource, p.Resource.Group, p.Name)
// }

type ApplySetTooling struct {
	Name    string
	Version string
}

func (t ApplySetTooling) String() string {
	return fmt.Sprintf("%s/%s", t.Name, t.Version)
}

// NewApplySet creates a new ApplySet object tracked by the given parent object.
func NewApplySet(parentRef ParentObjectRef, tooling ApplySetTooling, mapper meta.RESTMapper, client dynamic.Interface) *ApplySet {
	id := ComputeApplysetID(parentRef)

	return &ApplySet{
		currentResources:  make(map[schema.GroupKind]bool),
		currentNamespaces: make(sets.Set[string]),
		updatedResources:  make(map[schema.GroupKind]bool),
		updatedNamespaces: make(sets.Set[string]),
		id:                id,
		toolingID:         tooling,
		restMapper:        mapper,
		client:            client,
		visitedUids:       sets.New[types.UID](),
	}
}

const applySetIDPartDelimiter = "."

// ID is the label value that we are using to identify this applyset.
// Format: base64(sha256(<name>.<namespace>.<kind>.<group>)), using the URL safe encoding of RFC4648.

func ComputeApplysetID(parentRef ParentObjectRef) string {
	gvk := parentRef.GroupVersionKind()
	unencoded := strings.Join([]string{parentRef.GetName(), parentRef.GetNamespace(), gvk.Kind, gvk.Group}, applySetIDPartDelimiter)
	hashed := sha256.Sum256([]byte(unencoded))
	b64 := base64.RawURLEncoding.EncodeToString(hashed[:])
	// Label values must start and end with alphanumeric values, so add a known-safe prefix and suffix.
	return fmt.Sprintf(V1ApplySetIdFormat, b64)
}

// Validate imposes restrictions on the parent object that is used to track the applyset.
func (a ApplySet) Validate(ctx context.Context, client dynamic.Interface) error {
	var errors []error
	// if a.parentRef.IsNamespaced() && a.parentRef.Namespace == "" {
	// 	errors = append(errors, fmt.Errorf("namespace is required to use namespace-scoped ApplySet"))
	// }
	// if !builtinApplySetParentGVRs.Has(a.parentRef.Resource) {
	// 	// Determine which custom resource types are allowed as ApplySet parents.
	// 	// Optimization: Since this makes requests, we only do this if they aren't using a default type.
	// 	permittedCRParents, err := a.getAllowedCustomResourceParents(ctx, client)
	// 	if err != nil {
	// 		errors = append(errors, fmt.Errorf("identifying allowed custom resource parent types: %w", err))
	// 	}
	// 	parentRefResourceIgnoreVersion := a.parentRef.Resource.GroupResource().WithVersion("")
	// 	if !permittedCRParents.Has(parentRefResourceIgnoreVersion) {
	// 		errors = append(errors, fmt.Errorf("resource %q is not permitted as an ApplySet parent", a.parentRef.Resource))
	// 	}
	// }
	return utilerrors.NewAggregate(errors)
}

func (a *ApplySet) labelForCustomParentCRDs() *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      ApplysetParentCRDLabel,
			Operator: metav1.LabelSelectorOpExists,
		}},
	}
}

func (a *ApplySet) getAllowedCustomResourceParents(ctx context.Context, client dynamic.Interface) (sets.Set[schema.GroupVersionResource], error) {
	opts := metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(a.labelForCustomParentCRDs()),
	}
	list, err := client.Resource(schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	set := sets.New[schema.GroupVersionResource]()
	for i := range list.Items {
		// Custom resources must be named `<names.plural>.<group>`
		// and are served under `/apis/<group>/<version>/.../<plural>`
		gr := schema.ParseGroupResource(list.Items[i].GetName())
		set.Insert(gr.WithVersion(""))
	}
	return set, nil
}

func (a *ApplySet) LabelsForMember() map[string]string {
	return map[string]string{
		ApplysetPartOfLabel: a.id,
	}
}

// addLabels sets our tracking labels on each object; this should be called as part of loading the objects.
func (a *ApplySet) AddLabels(objects ...*resource.Info) error {
	applysetLabels := a.LabelsForMember()
	for _, obj := range objects {
		accessor, err := meta.Accessor(obj.Object)
		if err != nil {
			return fmt.Errorf("getting accessor: %w", err)
		}
		labels := accessor.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		for k, v := range applysetLabels {
			if _, found := labels[k]; found {
				return fmt.Errorf("ApplySet label %q already set in input data", k)
			}
			labels[k] = v
		}
		accessor.SetLabels(labels)
	}

	return nil
}

func (a *ApplySet) WithParent(ctx context.Context, parent ParentObject) error {
	// a.parentRef = parent
	// obj, err := a.parentRef.Read(ctx)

	// // helper := resource.NewHelper(a.client, a.parentRef.RESTMapping)
	// var dynamicResource dynamic.ResourceInterface
	// if a.parentRef.Namespace != "" {
	// 	dynamicResource = a.client.Resource(a.parentRef.Resource).Namespace(a.parentRef.Namespace)
	// } else {
	// 	dynamicResource = a.client.Resource(a.parentRef.Resource)
	// }
	// // obj, err := helper.Get(a.parentRef.Namespace, a.parentRef.Name)
	// obj, err := dynamicResource.Get(ctx, a.parentRef.Name, metav1.GetOptions{})
	// if errors.IsNotFound(err) {
	// 	// if !builtinApplySetParentGVRs.Has(a.parentRef.Resource) {
	// 	// 	return fmt.Errorf("custom resource ApplySet parents cannot be created automatically")
	// 	// }
	// 	return nil
	// } else if err != nil {
	// 	return fmt.Errorf("failed to fetch ApplySet parent object %q: %w", a.parentRef, err)
	// } else if obj == nil {
	// 	return fmt.Errorf("failed to fetch ApplySet parent object %q", a.parentRef)
	// }

	_, annotations, err := getLabelsAndAnnotations(parent)
	if err != nil {
		return fmt.Errorf("getting metadata from parent object %q: %w", parent, err)
	}

	// toolAnnotation, hasToolAnno := annotations[ApplySetToolingAnnotation]
	// if !hasToolAnno {
	// 	return fmt.Errorf("ApplySet parent object %q already exists and is missing required annotation %q", parent, ApplySetToolingAnnotation)
	// }
	// if managedBy := toolingBaseName(toolAnnotation); managedBy != a.toolingID.Name {
	// 	return fmt.Errorf("ApplySet parent object %q already exists and is managed by tooling %q instead of %q", parent, managedBy, a.toolingID.Name)
	// }

	// idLabel, hasIDLabel := labels[ApplySetParentIDLabel]
	// if !hasIDLabel {
	// 	return fmt.Errorf("ApplySet parent object %q exists and does not have required label %s", parent, ApplySetParentIDLabel)
	// }
	// if idLabel != a.id {
	// 	return fmt.Errorf("ApplySet parent object %q exists and has incorrect value for label %q (got: %s, want: %s)", parent, ApplySetParentIDLabel, idLabel, a.id)
	// }

	if a.currentResources, err = parseResourcesAnnotation(annotations, a.restMapper); err != nil {
		// TODO: handle GVRs for now-deleted CRDs
		return fmt.Errorf("parsing ApplySet annotation on %q: %w", parent, err)
	}
	a.currentNamespaces = parseNamespacesAnnotation(annotations)
	namespace := parent.GetNamespace()
	if namespace != "" {
		a.currentNamespaces.Insert(namespace)
	}
	return nil
}

func (a *ApplySet) LabelSelectorForMembers() string {
	return metav1.FormatLabelSelector(&metav1.LabelSelector{
		MatchLabels: a.LabelsForMember(),
	})
}

// AllPrunableResources returns the list of all resources that should be considered for pruning.
// This is potentially a superset of the resources types that actually contain resources.
func (a *ApplySet) AllPrunableResources() []schema.GroupKind {
	var ret []schema.GroupKind
	for gk := range a.currentResources {
		ret = append(ret, gk)
	}
	return ret
}

// AllPrunableNamespaces returns the list of all namespaces that should be considered for pruning.
// This is potentially a superset of the namespaces that actually contain resources.
func (a *ApplySet) AllPrunableNamespaces() []string {
	var ret []string
	for ns := range a.currentNamespaces {
		ret = append(ret, ns)
	}
	return ret
}

func getLabelsAndAnnotations(obj ParentObject) (map[string]string, map[string]string, error) {
	return obj.GetLabels(), obj.GetAnnotations(), nil
}

func toolingBaseName(toolAnnotation string) string {
	parts := strings.Split(toolAnnotation, "/")
	if len(parts) >= 2 {
		return strings.Join(parts[:len(parts)-1], "/")
	}
	return toolAnnotation
}

func parseResourcesAnnotation(annotations map[string]string, mapper meta.RESTMapper) (map[schema.GroupKind]bool, error) {
	annotation, ok := annotations[ApplySetGRsAnnotation]
	if !ok {
		// The spec does not require this annotation. However, 'missing' means 'perform discovery'.
		// We return an error because we do not currently support dynamic discovery in kubectl apply.
		// return nil, fmt.Errorf("kubectl requires the %q annotation to be set on all ApplySet parent objects", ApplySetGRsAnnotation)
	}
	mappings := make(map[schema.GroupKind]bool)
	// Annotation present but empty means that this is currently an empty set.
	if annotation == "" {
		return mappings, nil
	}
	for _, gkString := range strings.Split(annotation, ",") {
		gk := schema.ParseGroupKind(gkString)
		// gr := schema.ParseGroupResource(grString)
		// gvk, err := mapper.KindFor(gr.WithVersion(""))
		// if err != nil {
		// 	return nil, fmt.Errorf("invalid group resource in %q annotation: %w", ApplySetGRsAnnotation, err)
		// }
		// mapping, err := mapper.RESTMapping(gvk.GroupKind())
		// if err != nil {
		// 	return nil, fmt.Errorf("could not find kind for resource in %q annotation: %w", ApplySetGRsAnnotation, err)
		// }
		mappings[gk] = true
	}
	return mappings, nil
}

func parseNamespacesAnnotation(annotations map[string]string) sets.Set[string] {
	annotation, ok := annotations[ApplySetAdditionalNamespacesAnnotation]
	if !ok { // this annotation is completely optional
		return sets.Set[string]{}
	}
	// Don't include an empty namespace
	if annotation == "" {
		return sets.Set[string]{}
	}
	return sets.New(strings.Split(annotation, ",")...)
}

// addResource registers the given resource and namespace as being part of the updated set of
// resources being applied by the current operation.
func (a *ApplySet) addResource(gvk schema.GroupVersionKind, namespace string) {
	a.updatedResources[gvk.GroupKind()] = true
	if namespace != "" {
		a.updatedNamespaces.Insert(namespace)
	}
}

type ApplySetUpdateMode string

var updateToLatestSet ApplySetUpdateMode = "latest"
var updateToSuperset ApplySetUpdateMode = "superset"

func (a *ApplySet) updateParent(ctx context.Context, parent ParentObject, mode ApplySetUpdateMode, dryRun bool, validation string) error {
	data, err := json.Marshal(a.buildParentPatch(parent, mode))
	if err != nil {
		return fmt.Errorf("failed to encode patch for ApplySet parent: %w", err)
	}

	force := false
	patchOptions := metav1.PatchOptions{
		FieldValidation: validation,
		FieldManager:    a.FieldManager(),
		Force:           &force,
	}
	if dryRun {
		patchOptions.DryRun = []string{"All"}
	}
	err = parent.Patch(ctx, types.ApplyPatchType, data, patchOptions)
	if err != nil && errors.IsConflict(err) {
		// Try again with conflicts forced
		klog.Warningf("WARNING: failed to update ApplySet: %s\nApplySet field manager %s should own these fields. Retrying with conflicts forced.", err.Error(), a.FieldManager())
		force = true
		patchOptions.Force = &force
		err = parent.Patch(ctx, types.ApplyPatchType, data, patchOptions)
	}
	if err != nil {
		return fmt.Errorf("failed to update ApplySet: %w", err)
	}
	return nil
}

// func serverSideApplyRequest(ctx context.Context, a *ApplySet, data []byte, dryRun bool, validation string, forceConficts bool) error {
// 	if dryRun {
// 		return nil
// 	}
// 	var dynamicResource dynamic.ResourceInterface
// 	if a.parentRef.Namespace != "" {
// 		dynamicResource = a.client.Resource(a.parentRef.Resource).Namespace(a.parentRef.Namespace)
// 	} else {
// 		dynamicResource = a.client.Resource(a.parentRef.Resource)
// 	}

// 	// helper := resource.NewHelper(a.client, a.parentRef.RESTMapping).
// 	// 	DryRun(dryRun == cmdutil.DryRunServer).
// 	// 	WithFieldManager(a.FieldManager()).
// 	// 	WithFieldValidation(validation)

// 	options := metav1.PatchOptions{
// 		Force:           &forceConficts,
// 		FieldManager:    a.FieldManager(),
// 		FieldValidation: validation,
// 	}
// 	if dryRun {
// 		options.DryRun = []string{"All"}
// 	}
// 	_, err := dynamicResource.Patch(ctx, a.parentRef.Name, types.ApplyPatchType, data, options)

// 	// _, err := helper.Patch(
// 	// 	a.parentRef.Namespace,
// 	// 	a.parentRef.Name,
// 	// 	types.ApplyPatchType,
// 	// 	data,
// 	// 	&options,
// 	// )
// 	return err
// }

func (a *ApplySet) buildParentPatch(parent ParentObject, mode ApplySetUpdateMode) *unstructured.Unstructured {
	parentNamespace := parent.GetNamespace()
	parentName := parent.GetName()
	parentGVK := parent.GroupVersionKind()

	var newGRsAnnotation, newNsAnnotation string
	switch mode {
	case updateToSuperset:
		// If the apply succeeded but pruning failed, the set of group resources that
		// the ApplySet should track is the superset of the previous and current resources.
		// This ensures that the resources that failed to be pruned are not orphaned from the set.
		grSuperset := sets.KeySet(a.currentResources).Union(sets.KeySet(a.updatedResources))
		newGRsAnnotation = generateResourcesAnnotation(grSuperset)
		newNsAnnotation = generateNamespacesAnnotation(a.currentNamespaces.Union(a.updatedNamespaces), parentNamespace)
	case updateToLatestSet:
		newGRsAnnotation = generateResourcesAnnotation(sets.KeySet(a.updatedResources))
		newNsAnnotation = generateNamespacesAnnotation(a.updatedNamespaces, parentNamespace)
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(parentGVK)
	u.SetName(parentName)
	u.SetNamespace(parentNamespace)

	annotations := map[string]string{
		ApplySetToolingAnnotation:              a.toolingID.String(),
		ApplySetGRsAnnotation:                  newGRsAnnotation,
		ApplySetAdditionalNamespacesAnnotation: newNsAnnotation,
	}
	u.SetAnnotations(annotations)
	labels := map[string]string{
		ApplySetParentIDLabel: a.id,
	}
	u.SetLabels(labels)

	return u
	// return &metav1.PartialObjectMetadata{
	// 	TypeMeta: metav1.TypeMeta{
	// 		Kind:       parentGVK.Kind,
	// 		APIVersion: parentGVK.GroupVersion().String(),
	// 	},
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      parentName,
	// 		Namespace: parentNamespace,
	// 		Annotations: map[string]string{
	// 			ApplySetToolingAnnotation:              a.toolingID.String(),
	// 			ApplySetGRsAnnotation:                  newGRsAnnotation,
	// 			ApplySetAdditionalNamespacesAnnotation: newNsAnnotation,
	// 		},
	// 		Labels: map[string]string{
	// 			ApplySetParentIDLabel: a.id,
	// 		},
	// 	},
	// }
}

func generateNamespacesAnnotation(namespaces sets.Set[string], skip string) string {
	nsList := namespaces.Clone().Delete(skip).UnsortedList()
	sort.Strings(nsList)
	return strings.Join(nsList, ",")
}

func generateResourcesAnnotation(resources sets.Set[schema.GroupKind]) string {
	var ids []string
	for gk := range resources {
		ids = append(ids, gk.String())
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func (a ApplySet) FieldManager() string {
	return fmt.Sprintf("%s-applyset", a.toolingID.Name)
}

// MarkObjectVisited keeps track of UIDs of the applied
// objects. Used for pruning.
func (o *ApplySet) MarkObjectVisited(uid types.UID) error {

	o.visitedUids.Insert(uid)

	return nil
}

// // ParseApplySetParentRef creates a new ApplySetParentRef from a parent reference in the format [RESOURCE][.GROUP]/NAME
// func ParseApplySetParentRef(parentRefStr string, mapper meta.RESTMapper) (*ApplySetParentRef, error) {
// 	var gvr schema.GroupVersionResource
// 	var name string

// 	if groupRes, nameSuffix, hasTypeInfo := strings.Cut(parentRefStr, "/"); hasTypeInfo {
// 		name = nameSuffix
// 		gvr = schema.ParseGroupResource(groupRes).WithVersion("")
// 	} else {
// 		name = parentRefStr
// 		gvr = defaultApplySetParentGVR
// 	}

// 	if name == "" {
// 		return nil, fmt.Errorf("name cannot be blank")
// 	}

// 	gvk, err := mapper.KindFor(gvr)
// 	if err != nil {
// 		return nil, err
// 	}
// 	mapping, err := mapper.RESTMapping(gvk.GroupKind())
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &ApplySetParentRef{Name: name, RESTMapping: mapping}, nil
// }

// Prune deletes any objects from the apiserver that are no longer in the applyset.
func (a *ApplySet) Prune(ctx context.Context, parent ParentObject, validationDirective string, o *ApplySetDeleteOptions) error {
	if err := a.pruneAll(ctx, a.client, a.visitedUids, o); err != nil {
		return err
	}

	if err := a.updateParent(ctx, parent, updateToLatestSet, o.DryRun, validationDirective); err != nil {
		return fmt.Errorf("apply and prune succeeded, but ApplySet update failed: %w", err)
	}

	return nil
}

type ObjectInfo struct {
	Namespace string
	Name      string
	GVK       schema.GroupVersionKind
}

// BeforeApply should be called before applying the objects.
// It pre-updates the parent object so that it covers the resources that will be applied.
// In this way, even if we are interrupted, we will not leak objects.
func (a *ApplySet) BeforeApply(ctx context.Context, parent ParentObject, objects []ObjectInfo, dryRun bool, validationDirective string) error {
	if err := a.WithParent(ctx, parent); err != nil {
		return err
	}
	// Update the live parent object to the superset of the current and previous resources.
	// Doing this before the actual apply and prune operations improves behavior by ensuring
	// the live object contains the superset on failure. This may cause the next pruning
	// operation to make a larger number of GET requests than strictly necessary, but it prevents
	// object leakage from the set. The superset will automatically be reduced to the correct
	// set by the next successful operation.
	for _, info := range objects {
		a.addResource(info.GVK, info.Namespace)
	}
	if err := a.updateParent(ctx, parent, updateToSuperset, dryRun, validationDirective); err != nil {
		return err
	}
	return nil
}
