package watchset

import (
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

// TODO: Can we / should we fold this into applyset?  Similar data structure...

type status struct {
	mutex sync.RWMutex
	byGVK map[schema.GroupVersionKind]*gvkStatus
}

type gvkStatus struct {
	mutex           sync.RWMutex
	byNamespaceName map[types.NamespacedName]objectStatus
}

type objectStatus struct {
	deleted         bool
	resourceVersion string
}

func (s *status) deleteGVK(gvk schema.GroupVersionKind) {
	s.mutex.Lock()
	delete(s.byGVK, gvk)
	s.mutex.Unlock()
}

func (s *status) lastObserved(gvk schema.GroupVersionKind, nn types.NamespacedName) (objectStatus, bool) {
	s.mutex.RLock()
	gvkStatus := s.byGVK[gvk]
	s.mutex.RUnlock()

	if gvkStatus == nil {
		return objectStatus{}, false
	}
	return gvkStatus.lastObserved(nn)
}

func (s *status) event(ev watch.Event) {
	obj := ev.Object
	gvk := obj.GetObjectKind().GroupVersionKind()

	s.mutex.RLock()
	status := s.byGVK[gvk]
	s.mutex.RUnlock()

	if status == nil {
		s.mutex.Lock()
		status = s.byGVK[gvk]
		if status == nil {
			status = &gvkStatus{
				byNamespaceName: make(map[types.NamespacedName]objectStatus),
			}
			s.byGVK[gvk] = status
		}
		s.mutex.Unlock()
	}

	// TODO: Pass status to watcher?

	status.event(ev)
}

func (s *gvkStatus) event(ev watch.Event) {
	obj := ev.Object
	accessor, err := meta.Accessor(obj)
	if err != nil {
		klog.Fatalf("failed to get accessor for %T: %w", obj, err)
	}

	// TODO: If delete event, just delete?  Otherwise map grows forever.

	nn := types.NamespacedName{
		Namespace: accessor.GetNamespace(),
		Name:      accessor.GetName(),
	}

	objectStatus := buildObjectStatus(ev, accessor)
	s.mutex.Lock()
	s.byNamespaceName[nn] = objectStatus
	s.mutex.Unlock()
}

func (s *gvkStatus) lastObserved(nn types.NamespacedName) (objectStatus, bool) {
	s.mutex.RLock()
	objectStatus, ok := s.byNamespaceName[nn]
	s.mutex.RUnlock()

	return objectStatus, ok
}

func buildObjectStatus(ev watch.Event, accessor metav1.Object) objectStatus {
	// TODO: Locking.  Or maybe don't update in place?

	var s objectStatus

	if ev.Type == watch.Deleted {
		s.deleted = true
	}

	rv := accessor.GetResourceVersion()
	s.resourceVersion = rv

	// TODO: Should we recompute helath here, or should we simply reapply?

	return s
}

func (s *objectStatus) ResourceVersion() string {
	return s.resourceVersion
}
