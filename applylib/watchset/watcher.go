package watchset

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

type Watcher struct {
	client     dynamic.Interface
	restMapper meta.RESTMapper

	status *status

	mutex        sync.Mutex
	interests    []*InterestSet
	watchesByGVK map[schema.GroupVersionKind]*gvkWatcher
}

type WatcherOptions struct {
	Client     dynamic.Interface
	RESTMapper meta.RESTMapper
}

func New(options WatcherOptions) *Watcher {
	w := &Watcher{
		client:     options.Client,
		restMapper: options.RESTMapper,
	}

	w.watchesByGVK = make(map[schema.GroupVersionKind]*gvkWatcher)
	w.status = &status{
		byGVK: make(map[schema.GroupVersionKind]*gvkStatus),
	}

	return w
}

func (w *Watcher) LastObserved(gvk schema.GroupVersionKind, nn types.NamespacedName) (objectStatus, bool) {
	return w.status.lastObserved(gvk, nn)
}

func (w *Watcher) NewInterestSet() *InterestSet {
	interest := &InterestSet{parent: w}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.interests = append(w.interests, interest)

	return interest
}

func (w *Watcher) updateInterests() {
	interestByGVK := make(map[schema.GroupVersionKind]bool)

	w.mutex.Lock()
	defer w.mutex.Unlock()
	for _, interest := range w.interests {
		if interest.closed {
			// TODO: Clean up closed InterestSets from the slice
			continue
		}
		for _, gvk := range interest.gvks {
			interestByGVK[gvk] = true
		}
	}

	// Make sure we are running watches for all interests
	for gvk := range interestByGVK {
		watcher := w.watchesByGVK[gvk]
		if watcher == nil {
			watcher = &gvkWatcher{
				gvk:        gvk,
				parent:     w,
				status:     w.status,
				restMapper: w.restMapper,
				client:     w.client,
			}

			ctx, cancel := context.WithCancel(context.Background())
			watcher.cancel = cancel

			go watcher.watchForever(ctx)

			w.watchesByGVK[gvk] = watcher
		}
	}

	// Close down any watches no longer of interest
	for gvk, watcher := range w.watchesByGVK {
		if interestByGVK[gvk] {
			continue
		}

		watcher.cancel()
		delete(w.watchesByGVK, gvk)
		w.status.deleteGVK(gvk)
	}

}

type gvkWatcher struct {
	client     dynamic.Interface
	restMapper meta.RESTMapper

	// TODO: Should we replace this with a direct pointer to the target(s)?
	parent *Watcher

	status *status
	gvk    schema.GroupVersionKind
	gvr    schema.GroupVersionResource

	cancel func()
}

func (w *gvkWatcher) watchForever(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := w.watchOnce(ctx); err != nil {
			klog.Warningf("error from watch; will reconect: %v", err)
		} else {
			klog.Warningf("watch closed; will reconect")
		}
		time.Sleep(5 * time.Second)
	}
}

func (w *gvkWatcher) watchOnce(ctx context.Context) error {
	if w.gvr.Empty() {
		restMapping, err := w.restMapper.RESTMapping(w.gvk.GroupKind(), w.gvk.Version)
		if err != nil {
			return fmt.Errorf("unable to get rest mapping for %v: %w", w.gvk, err)
		}
		w.gvr = restMapping.Resource
	}

	listOptions := metav1.ListOptions{
		Watch:               true,
		AllowWatchBookmarks: true,
		// TODO: ResourceVersion (maybe)
	}
	// TODO: Can we use metadata only (probably not if we have to compute the health)
	watcher, err := w.client.Resource(w.gvr).Watch(ctx, listOptions)
	if err != nil {
		return fmt.Errorf("failed to start watch: %w", err)
	}

	// Always clean up
	defer watcher.Stop()

	for ev := range watcher.ResultChan() {
		switch ev.Type {
		case watch.Bookmark:
			// TODO: Update resource version?

		case watch.Error:
			klog.Warningf("got error on watch stream: %v", ev)
			return fmt.Errorf("got error on watch stream")

		case watch.Added, watch.Modified, watch.Deleted:
			w.status.event(ev)

		default:
			klog.Warningf("got unknown message on watch stream: %v", ev)
			return fmt.Errorf("got unknown message watch stream")

		}
	}

	return nil
}
