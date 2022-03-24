package status

import (
	"context"
	"fmt"
	"reflect"

	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/addon/pkg/apis/v1alpha1"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/addon/pkg/utils"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative"
	"sigs.k8s.io/kubebuilder-declarative-pattern/pkg/patterns/declarative/pkg/manifest"
)

type kstatusAggregator struct {
	client     client.Client
	reconciler *declarative.Reconciler
}

func NewKstatusAgregator(c client.Client, reconciler *declarative.Reconciler) *kstatusAggregator {
	return &kstatusAggregator{client: c, reconciler: reconciler}
}

func (k *kstatusAggregator) Reconciled(ctx context.Context, src declarative.DeclarativeObject,
	objs *manifest.Objects, err error) error {
	log := log.Log

	if err != nil {
		log.Info("not updating aggregation status, because of error", "error", err)
		return nil
	}

	statusMap := make(map[status.Status]bool)
	if objs != nil {
		for _, object := range objs.Items {

			unstruct, err := declarative.GetObjectFromCluster(object, k.reconciler)
			if err != nil {
				log.WithValues("object", object.Kind+"/"+object.GetName()).Error(err, "Unable to get status of object")
				return err
			}

			res, err := status.Compute(unstruct)
			if err != nil {
				log.WithValues("kind", object.Kind).WithValues("name", object.GetName()).WithValues("status", res.Status).WithValues(
					"message", res.Message).Info("Got status of resource:")
				statusMap[status.NotFoundStatus] = true
			}
			if res != nil {
				log.WithValues("kind", object.Kind).WithValues("name", object.GetName()).WithValues("status", res.Status).WithValues("message", res.Message).Info("Got status of resource:")
				statusMap[res.Status] = true
			}
		}
	}

	currentStatus, err := utils.GetCommonStatus(src)
	if err != nil {
		log.Error(err, "error retrieving status")
		return err
	}

	newStatus := currentStatus
	aggregateStatus(statusMap, &newStatus)

	if !reflect.DeepEqual(newStatus, currentStatus) {
		err := utils.SetCommonStatus(src, newStatus)
		if err != nil {
			return err
		}
		log.WithValues("name", src.GetName()).WithValues("status", newStatus).Info("updating status")
		err = k.client.Status().Update(ctx, src)
		if err != nil {
			log.Error(err, "error updating status")
			return fmt.Errorf("error updating status: %w", err)
		}
	}

	return nil
}

func aggregateStatus(m map[status.Status]bool, newStatus *v1alpha1.CommonStatus) {
	inProgress := m[status.InProgressStatus]
	terminating := m[status.TerminatingStatus]

	failed := m[status.FailedStatus]

	if inProgress || terminating {
		newStatus.Healthy = false
		newStatus.Phase = string(status.InProgressStatus)
		return
	}

	if failed {
		newStatus.Healthy = false
		newStatus.Phase = string(status.FailedStatus)
		return
	}

	newStatus.Healthy = true
	newStatus.Phase = string(status.CurrentStatus)
	return
}
