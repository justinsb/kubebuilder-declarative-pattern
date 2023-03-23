package watchset

import (
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// InterestSet represents a client that wants to know the ongoing status of a set of objects.
// The guarantee is that once we call ReplaceAllObjects, we will try to watch objects from that point onwards.
// TODO: Should we have a status on the InterestSet, to indicate that all our watches are healthy?
// TODO: Can we / should we fold this into applyset?  Similar data structure...
type InterestSet struct {
	parent *Watcher

	mutex  sync.Mutex
	closed bool
	gvks   []schema.GroupVersionKind
}

// ReplaceAllObjects completely replaces all the objects of interest.
func (s *InterestSet) ReplaceAllObjects(objects []*unstructured.Unstructured) {
	gvks := transform(objects, func(u *unstructured.Unstructured) schema.GroupVersionKind { return u.GroupVersionKind() })
	gvkList := unique(gvks)

	s.mutex.Lock()
	s.gvks = gvkList
	s.mutex.Unlock()

	s.parent.updateInterests()
}

// Close indicates that this InterestSet is no longer in use.
func (s *InterestSet) Close() {
	s.mutex.Lock()
	s.closed = true
	s.gvks = nil
	s.mutex.Unlock()

	s.parent.updateInterests()
}
