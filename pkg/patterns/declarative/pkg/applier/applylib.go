package applier

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"sigs.k8s.io/kubebuilder-declarative-pattern/applylib/applyset"
	"sigs.k8s.io/kubebuilder-declarative-pattern/applylib/applyset/prune"
)

type ApplySetApplier struct {
	patchOptions metav1.PatchOptions
}

var _ Applier = &ApplySetApplier{}

func NewApplySetApplier(patchOptions metav1.PatchOptions) *ApplySetApplier {
	return &ApplySetApplier{patchOptions: patchOptions}
}

func (a *ApplySetApplier) Apply(ctx context.Context, opt ApplierOptions) error {
	dryRun := false
	validationDirective := metav1.FieldValidationWarn

	patchOptions := a.patchOptions

	for _, arg := range opt.ExtraArgs {
		switch arg {
		case "--force":
			opt.Force = true

		default:
			return fmt.Errorf("extraArg %q is not supported by the ApplySetApplier", arg)
		}
	}

	patchOptions.Force = &opt.Force

	dynamicClient, err := dynamic.NewForConfig(opt.RESTConfig)
	if err != nil {
		return fmt.Errorf("error building dynamic client: %w", err)
	}

	restMapper := opt.RESTMapper

	options := applyset.Options{
		PatchOptions: patchOptions,
		RESTMapper:   restMapper,
		Client:       dynamicClient,
	}
	s, err := applyset.New(options)
	if err != nil {
		return fmt.Errorf("error creating applyset: %w", err)
	}

	// Populate the namespace on any namespace-scoped objects
	if opt.Namespace != "" {
		for _, obj := range opt.Objects {
			gvk := obj.GroupVersionKind()
			restMapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			if err != nil {
				return fmt.Errorf("error getting rest mapping for %v: %w", gvk, err)
			}

			switch restMapping.Scope {
			case meta.RESTScopeNamespace:
				obj.SetNamespace(opt.Namespace)

			case meta.RESTScopeRoot:
				// Don't set namespace
			default:
				return fmt.Errorf("unknown rest mapping scope %v", restMapping.Scope)
			}
		}
	}

	var pruner *prune.ApplySet

	if opt.ApplysetParent != nil {
		parentGVK := opt.ApplysetParent.GroupVersionKind()
		parentRESTMapping, err := restMapper.RESTMapping(parentGVK.GroupKind(), parentGVK.Version)
		if err != nil {
			return fmt.Errorf("error getting parent rest mapping for %v: %w", parentRESTMapping, err)
		}

		// parent := &prune.ApplySetParentRef{
		// 	Name:        opt.ApplysetParent.GetName(),
		// 	Namespace:   opt.ApplysetParent.GetNamespace(),
		// 	RESTMapping: parentRESTMapping,
		// }

		tooling := opt.ApplysetTooling

		pruner = prune.NewApplySet(opt.ApplysetParent, tooling, restMapper, dynamicClient)
	}

	var applyableObjects []applyset.ApplyableObject
	if pruner != nil {
		addLabels := pruner.LabelsForMember()

		var info []prune.ObjectInfo

		for _, obj := range opt.Objects {
			u := obj.UnstructuredObject()
			gvk := obj.GroupVersionKind()
			labels := u.GetLabels()
			if labels == nil {
				labels = make(map[string]string)
			}
			for k, v := range addLabels {
				labels[k] = v
			}
			u.SetLabels(labels)
			applyableObjects = append(applyableObjects, u)
			info = append(info, prune.ObjectInfo{
				Namespace: u.GetNamespace(),
				Name:      u.GetName(),
				GVK:       gvk,
			})
		}

		if err := pruner.BeforeApply(ctx, opt.ApplysetParent, info, dryRun, validationDirective); err != nil {
			return fmt.Errorf("error updating parent before apply: %w", err)
		}
	} else {
		for _, obj := range opt.Objects {
			applyableObject := obj.UnstructuredObject()
			applyableObjects = append(applyableObjects, applyableObject)
		}
	}

	if err := s.SetDesiredObjects(applyableObjects); err != nil {
		return fmt.Errorf("error setting desired objects for apply: %w", err)
	}

	results, err := s.ApplyOnce(ctx)
	if err != nil {
		// TODO: Aggregate errors?
		return fmt.Errorf("error applying objects: %w", err)
	}
	if !results.AllApplied() {
		return fmt.Errorf("not all objects applied")
	}

	// TODO: Check healthy

	if pruner != nil && results.AllApplied() && results.AllHealthy() {
		for uid := range results.AppliedUIDs() {
			pruner.MarkObjectVisited(uid)
		}

		deleteOptions := &prune.ApplySetDeleteOptions{
			// CascadingStrategy: ,
			// DryRunStrategy    cmdutil.DryRunStrategy
			// GracePeriod       int
		}

		if err := pruner.Prune(ctx, opt.ApplysetParent, validationDirective, deleteOptions); err != nil {
			return fmt.Errorf("error deleting objects: %w", err)
		}
	}

	return nil
}
