package status

import (
	"context"
	"fmt"

	"github.com/blang/semver"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	//"sigs.k8s.io/controller-runtime/pkg/log"
	appsv1beta1 "sigs.k8s.io/application/api/v1beta1"
	addonsv1alpha1 "sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/addon/pkg/apis/v1alpha1"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative/pkg/manifest"
)

// NewAggregator provides an implementation of declarative.Reconciled that
// aggregates the status of deployed objects to configure the 'Healthy'
// field on an addon that derives from CommonStatus
func NewPreflightChecks(client client.Client, version string) *preflightChecks {
	return &preflightChecks{client, version}
}

type preflightChecks struct {
	client  client.Client
	version string
}

func (p *preflightChecks) Preflight(ctx context.Context, src declarative.DeclarativeObject, objs *manifest.Objects) error {
	log := log.Log
	var versionNeededStr string

	// get Annotation from Application resource
	for _, obj := range objs.Items {
		if obj.Kind == "Application" {
			newApp := appsv1beta1.Application{}

			unstruct := obj.UnstructuredObject().Object
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstruct, &newApp)

			if err != nil {
				log.WithValues("Object", unstruct).Info("Unable to unmarshall Application, %v", err)
				continue
			}

			// metadata := unstruct["metadata"]["annotations"][]

			versionNeededStr = newApp.ObjectMeta.Annotations["addons.k8s.io/operator-version"]

		}
	}

	if versionNeededStr != "" {
		versionNeeded, err := semver.Make(versionNeededStr)
		// TODO(somtochi): Do we want to return an error when the version is invalid or just skip and use the operator?
		if err != nil {
			log.WithValues("version", versionNeededStr).Info("Unable to convert string to version, skipping check")
			return nil
		}
		operatorVersion, err := semver.Make(p.version)
		if err != nil {
			log.WithValues("version", p.version).Info("Unable to convert string to version, skipping check")
			return nil
		}

		if versionNeeded.GT(operatorVersion) {
			addonObject, ok := src.(addonsv1alpha1.CommonObject)
			if !ok {
				return fmt.Errorf("object %T was not an addonsv1alpha1.CommonObject", src)
			}

			status := addonsv1alpha1.CommonStatus{
				Healthy: false,
				Errors: []string{
					fmt.Sprintf("Addons needs version %v, this operator is version %v", versionNeeded.String(), operatorVersion.String()),
				},
			}
			addonObject.SetCommonStatus(status)

			return fmt.Errorf("Operator not qualified, manifest needs operator >= %v", versionNeeded.String())
		}
	}

	return nil
}
