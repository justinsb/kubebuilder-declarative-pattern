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

package watch

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// WatchDelay is the time between a Watch being dropped and attempting to resume it
const WatchDelay = 30 * time.Second

type DynamicWatch interface {
	// Add registers a watch for changes to 'trigger' filtered by 'options' to raise an event on 'target'
	Add(trigger schema.GroupVersionKind, options metav1.ListOptions, target metav1.ObjectMeta) error

	// AddForGVR registers a watch for changes to 'trigger' filtered by 'options' to raise an event on 'target'
	AddForGVR(ctx context.Context, trigger schema.GroupVersionResource, options metav1.ListOptions, callback func(watch.Event)) error
}

func NewDynamicWatch(config rest.Config) (DynamicWatch, chan event.GenericEvent, error) {
	dw := &dynamicWatch{events: make(chan event.GenericEvent)}

	restMapper, err := apiutil.NewDiscoveryRESTMapper(&config)
	if err != nil {
		return nil, nil, err
	}

	client, err := dynamic.NewForConfig(&config)
	if err != nil {
		return nil, nil, err
	}

	dw.restMapper = restMapper
	dw.config = config
	dw.client = client
	return dw, dw.events, nil
}

type dynamicWatch struct {
	config     rest.Config
	client     dynamic.Interface
	restMapper meta.RESTMapper
	events     chan event.GenericEvent
}

func (dw *dynamicWatch) newDynamicClient(gvr schema.GroupVersionResource) (dynamic.ResourceInterface, error) {
	return dw.client.Resource(gvr), nil
}

// Add registers a watch for changes to 'trigger' filtered by 'options' to raise an event on 'target'
func (dw *dynamicWatch) Add(trigger schema.GroupVersionKind, options metav1.ListOptions, target metav1.ObjectMeta) error {
	ctx := context.TODO()

	mapping, err := dw.restMapper.RESTMapping(trigger.GroupKind(), trigger.Version)
	if err != nil {
		return err
	}

	callback := func(ev watch.Event) {
		dw.events <- event.GenericEvent{Object: clientObject{Object: ev.Object, ObjectMeta: &target}}
	}
	return dw.AddForGVR(ctx, mapping.Resource, options, callback)
}

// AddForGVR registers a watch for changes to 'trigger' filtered by 'options' to raise an event on 'target'
func (dw *dynamicWatch) AddForGVR(ctx context.Context, trigger schema.GroupVersionResource, options metav1.ListOptions, callback func(watch.Event)) error {
	client, err := dw.newDynamicClient(trigger)
	if err != nil {
		return fmt.Errorf("creating client for (%s): %v", trigger.String(), err)
	}

	go func() {
		for {
			dw.watchUntilClosed(ctx, client, trigger, options, callback)

			time.Sleep(WatchDelay)
		}
	}()

	return nil
}

var _ client.Object = clientObject{}

// clientObject is a concrete client.Object to pass to watch events.
type clientObject struct {
	runtime.Object
	*metav1.ObjectMeta
}

// A Watch will be closed when the pod loses connection to the API server.
// If a Watch is opened with no ResourceVersion then we will recieve an 'ADDED'
// event for all Watch objects[1]. This will result in 'overnotification'
// from this Watch but it will ensure we always Reconcile when needed`.
//
// [1] https://github.com/kubernetes/kubernetes/issues/54878#issuecomment-357575276
func (dw *dynamicWatch) watchUntilClosed(ctx context.Context, client dynamic.ResourceInterface, trigger schema.GroupVersionResource, options metav1.ListOptions, callback func(watch.Event)) {
	log := log.Log

	events, err := client.Watch(ctx, options)
	if err != nil {
		log.WithValues("options", options).WithValues("trigger", trigger).Error(err, "adding watch to dynamic client")
		return
	}

	log.WithValues("trigger", trigger.String()).WithValues("options", options).Info("watch began")

	// Always clean up watchers
	defer events.Stop()

	for ev := range events.ResultChan() {
		log.WithValues("type", ev.Type).WithValues("trigger", trigger.String()).Info("broadcasting event")
		callback(ev)
	}

	log.WithValues("trigger", trigger.String()).WithValues("options", options).Info("watch closed")

	return
}
