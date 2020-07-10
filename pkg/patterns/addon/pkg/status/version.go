package status

import (
	"context"
	"fmt"

	"github.com/blang/semver"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	addonsv1alpha1 "sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/addon/pkg/apis/v1alpha1"

	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative/pkg/manifest"
)

// NewVersionCheck provides an implementation of declarative.Reconciled that
// checks the version of the operator if it is up to the version required by the manifest
func NewVersionCheck(client client.Client, version string) (*versionCheck, error) {
	log := log.Log
	operatorVersion, err := semver.Make(version)
	if err != nil {
		log.WithValues("version", version).Info("Unable to convert string to version, skipping check")
		return nil, err
	}
	return &versionCheck{client, operatorVersion}, nil
}

type versionCheck struct {
	client  client.Client
	version semver.Version
}

func (p *versionCheck) VersionCheck(
	ctx context.Context,
	src declarative.DeclarativeObject,
	objs *manifest.Objects,
) (bool, error) {
	log := log.Log
	zeroVersion := semver.Version{}
	maxVersion := zeroVersion

	// Look for annotation from any resource with the max version
	for _, obj := range objs.Items {
		annotations := obj.UnstructuredObject().GetAnnotations()
		if versionNeededStr, ok := annotations["addons.k8s.io/operator-version"]; ok {
			log.WithValues("version", versionNeededStr).Info("Got version, %v")

			versionActual, err := semver.Make(versionNeededStr)
			if err != nil {
				log.WithValues("version", versionNeededStr).Error(err, "Unable to convert string to version, skipping this object")
				return false, err
			}

			if versionActual.GT(maxVersion) {
				maxVersion = versionActual
			}
		}
	}

	if maxVersion.Equals(zeroVersion) || !maxVersion.GT(p.version) {
		return true, nil
	}

	addonObject, ok := src.(addonsv1alpha1.CommonObject)
	if !ok {
		return false, fmt.Errorf("object %T was not an addonsv1alpha1.CommonObject", src)
	}

	status := addonsv1alpha1.CommonStatus{
		Healthy: false,
		Errors: []string{
			fmt.Sprintf("Addons needs version %v, this operator is version %v", maxVersion.String(), p.version.String()),
		},
	}
	log.WithValues("name", addonObject.GetName()).WithValues("status", status).Info("updating status")
	addonObject.SetCommonStatus(status)

	return false, fmt.Errorf("operator not qualified, manifest needs operator >= %v", maxVersion.String())
}
