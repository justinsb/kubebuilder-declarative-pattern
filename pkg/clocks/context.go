/*
Copyright 2023 The Kubernetes Authors.

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

package clocks

import (
	"context"

	"k8s.io/utils/clock"
)

// contextKey is how we find clocks in a context.Context.
type contextKey struct{}

// FromContext returns a clock from a context.Context, defaulting to clock.RealClock.
func FromContext(ctx context.Context) clock.Clock {
	if ctx != nil {
		if v, ok := ctx.Value(contextKey{}).(clock.Clock); ok {
			return v
		}
	}
	return &clock.RealClock{}
}

// IntoContext takes a context and sets the clock into it.
// Use FromContext function to retrieve the clock.
func IntoContext(ctx context.Context, clock clock.Clock) context.Context {
	return context.WithValue(ctx, contextKey{}, clock)
}
