package status

import (
	"context"
	"fmt"

	"github.com/blang/semver"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	//"sigs.k8s.io/controller-runtime/pkg/log"

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

func (p *preflightChecks) Preflight(
	ctx context.Context,
	src declarative.DeclarativeObject,
	objs *manifest.Objects,
) error {
	log := log.Log
	baseVersion := semver.Version{}
	var versionNeededStr string
	maxVersion := baseVersion

	// get Annotation from any resource[Do we just want to do this on Applicationn?]
	for _, obj := range objs.Items {
		// if obj.Kind == "Application" {
		// newApp := appsv1beta1.Application{}

		unstruct := obj.UnstructuredObject().Object
		metadata := unstruct["metadata"].(map[string]interface{})
		annotations, ok := metadata["annotations"].(map[string]interface{})
		if ok {
			versionNeededStr, _ = annotations["addons.k8s.io/operator-version"].(string)
			log.WithValues("version", versionNeededStr).Info("Got version, %v")

			versionActual, err := semver.Make(versionNeededStr)
			if err != nil {
				log.WithValues("version", versionNeededStr).Info("Unable to convert string to version, skipping this object")
				continue
			}

			if versionActual.GT(maxVersion) {
				maxVersion = versionActual
			}
		}
	}
	if !maxVersion.Equals(baseVersion) {
		// TODO(somtochi): Do we want to return an error when the version is invalid or just skip and use the operator?
		operatorVersion, err := semver.Make(p.version)
		if err != nil {
			log.WithValues("version", p.version).Info("Unable to convert string to version, skipping check")
			return nil
		}

		if maxVersion.GT(operatorVersion) {
			addonObject, ok := src.(addonsv1alpha1.CommonObject)
			if !ok {
				return fmt.Errorf("object %T was not an addonsv1alpha1.CommonObject", src)
			}

			status := addonsv1alpha1.CommonStatus{
				Healthy: false,
				Errors: []string{
					fmt.Sprintf("Addons needs version %v, this operator is version %v", maxVersion.String(), operatorVersion.String()),
				},
			}
			log.WithValues("name", instance.GetName()).WithValues("status", status).Info("updating status")
			addonObject.SetCommonStatus(status)

			return fmt.Errorf("Operator not qualified, manifest needs operator >= %v", maxVersion.String())
		}
	}

	return nil
}
