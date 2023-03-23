/*
Copyright 2022 The Kubernetes Authors.

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

package applyset

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// ApplyResults contains the results of an Apply operation.
type ApplyResults struct {
	total          int
	successObjects []objectInfo
	applyFailCount int
	healthyCount   int
	unhealthyCount int
}

type objectInfo struct {
	id  types.NamespacedName
	gvk schema.GroupVersionKind
	uid types.UID
}

// AllApplied is true if the desired state has been successfully applied for all objects.
// Note: you likely also want to check AllHealthy, if you want to be sure the objects are "ready".
func (r *ApplyResults) AllApplied() bool {
	r.checkInvariants()

	return r.applyFailCount == 0
}

// AppliedUIDs is the set of all applied object UIDs
func (r *ApplyResults) AppliedUIDs() sets.Set[types.UID] {
	out := sets.New[types.UID]()
	for _, obj := range r.successObjects {
		out.Insert(obj.uid)
	}
	return out
}

// AllHealthy is true if all the objects have been applied and have converged to a "ready" state.
// Note that this is only meaningful if AllApplied is true.
func (r *ApplyResults) AllHealthy() bool {
	r.checkInvariants()

	return r.unhealthyCount == 0
}

// checkInvariants is an internal function that warns if the object doesn't match the expected invariants.
func (r *ApplyResults) checkInvariants() {
	applySuccessCount := len(r.successObjects)
	if r.total != (applySuccessCount + r.applyFailCount) {
		klog.Warningf("consistency error (apply counts): %#v", r)
	} else if r.total != (r.healthyCount + r.unhealthyCount) {
		// This "invariant" only holds when all objects could be applied
		klog.Warningf("consistency error (healthy counts): %#v", r)
	}
}

// applyError records that the apply of an object failed with an error.
func (r *ApplyResults) applyError(gvk schema.GroupVersionKind, nn types.NamespacedName, err error) {
	r.applyFailCount++
	klog.Warningf("error from apply on %s %s: %v", gvk, nn, err)
}

// afterApplySuccess records that an object was applied and this succeeded.
func (r *ApplyResults) afterApplySuccess(gvk schema.GroupVersionKind, nn types.NamespacedName, u *unstructured.Unstructured) {
	r.successObjects = append(r.successObjects, objectInfo{
		gvk: gvk,
		id:  nn,
		uid: u.GetUID(),
	})
}

// reportHealth records the health of an object.
func (r *ApplyResults) reportHealth(gvk schema.GroupVersionKind, nn types.NamespacedName, isHealthy bool) {
	if isHealthy {
		r.healthyCount++
	} else {
		r.unhealthyCount++
	}
}
