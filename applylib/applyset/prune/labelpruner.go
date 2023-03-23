package prune

// import (
// 	"context"
// 	"fmt"
// 	"sync"

// 	"k8s.io/apimachinery/pkg/api/meta"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"k8s.io/apimachinery/pkg/types"
// 	"k8s.io/apimachinery/pkg/util/sets"
// 	"k8s.io/client-go/dynamic"
// 	"k8s.io/klog/v2"
// 	cmdutil "k8s.io/kubectl/pkg/cmd/util"
// )

// type LabelPruner struct {
// }

// type ApplySetDeleteOptions struct {
// 	CascadingStrategy metav1.DeletionPropagation
// 	DryRunStrategy    cmdutil.DryRunStrategy
// 	GracePeriod       int

// 	// Printer printers.ResourcePrinter

// 	// IOStreams genericclioptions.IOStreams
// }

// // PruneObject is an apiserver object that should be deleted as part of prune.
// type PruneObject struct {
// 	Name      string
// 	Namespace string
// 	Mapping   *meta.RESTMapping
// 	Object    runtime.Object
// }

// // String returns a human-readable name of the object, for use in debug messages.
// func (p *PruneObject) String() string {
// 	s := p.Mapping.GroupVersionKind.GroupKind().String()

// 	if p.Namespace != "" {
// 		s += " " + p.Namespace + "/" + p.Name
// 	} else {
// 		s += " " + p.Name
// 	}
// 	return s
// }

// // FindAllObjectsToPrune returns the list of objects that will be pruned.
// // Calling this instead of Prune can be useful for dry-run / diff behaviour.
// func (a *LabelPruner) FindAllObjectsToPrune(ctx context.Context, dynamicClient dynamic.Interface, visitedUids sets.Set[types.UID]) ([]PruneObject, error) {
// 	type task struct {
// 		namespace   string
// 		restMapping *meta.RESTMapping

// 		err     error
// 		results []PruneObject
// 	}
// 	var tasks []*task

// 	// We run discovery in parallel, in as many goroutines as priority and fairness will allow
// 	// (We don't expect many requests in real-world scenarios - maybe tens, unlikely to be hundreds)
// 	for _, restMapping := range a.AllPrunableResources() {
// 		switch restMapping.Scope.Name() {
// 		case meta.RESTScopeNameNamespace:
// 			for _, namespace := range a.AllPrunableNamespaces() {
// 				if namespace == "" {
// 					// Just double-check because otherwise we get cryptic error messages
// 					return nil, fmt.Errorf("unexpectedly encountered empty namespace during prune of namespace-scoped resource %v", restMapping.GroupVersionKind)
// 				}
// 				tasks = append(tasks, &task{
// 					namespace:   namespace,
// 					restMapping: restMapping,
// 				})
// 			}

// 		case meta.RESTScopeNameRoot:
// 			tasks = append(tasks, &task{
// 				restMapping: restMapping,
// 			})

// 		default:
// 			return nil, fmt.Errorf("unhandled scope %q", restMapping.Scope.Name())
// 		}
// 	}

// 	var wg sync.WaitGroup

// 	for i := range tasks {
// 		task := tasks[i]
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()

// 			results, err := a.findObjectsToPrune(ctx, dynamicClient, visitedUids, task.namespace, task.restMapping)
// 			if err != nil {
// 				task.err = fmt.Errorf("listing %v objects for pruning: %w", task.restMapping.GroupVersionKind.String(), err)
// 			} else {
// 				task.results = results
// 			}
// 		}()
// 	}
// 	// Wait for all the goroutines to finish
// 	wg.Wait()

// 	var allObjects []PruneObject
// 	for _, task := range tasks {
// 		if task.err != nil {
// 			return nil, task.err
// 		}
// 		allObjects = append(allObjects, task.results...)
// 	}
// 	return allObjects, nil
// }

// func (a *LabelPruner) pruneAll(ctx context.Context, dynamicClient dynamic.Interface, visitedUids sets.Set[types.UID], deleteOptions *ApplySetDeleteOptions) error {
// 	allObjects, err := a.FindAllObjectsToPrune(ctx, dynamicClient, visitedUids)
// 	if err != nil {
// 		return err
// 	}

// 	return a.DeleteObjects(ctx, dynamicClient, allObjects, deleteOptions)
// }

// // const applySetIDPartDelimiter = "."

// // // ID is the label value that we are using to identify this applyset.
// // // Format: base64(sha256(<name>.<namespace>.<kind>.<group>)), using the URL safe encoding of RFC4648.

// // func (a *Pruner) ID() string {
// // 	unencoded := strings.Join([]string{a.parentRef.Name, a.parentRef.Namespace, a.parentRef.GroupVersionKind.Kind, a.parentRef.GroupVersionKind.Group}, applySetIDPartDelimiter)
// // 	hashed := sha256.Sum256([]byte(unencoded))
// // 	b64 := base64.RawURLEncoding.EncodeToString(hashed[:])
// // 	// Label values must start and end with alphanumeric values, so add a known-safe prefix and suffix.
// // 	return fmt.Sprintf(V1ApplySetIdFormat, b64)
// // }

// func (a *LabelPruner) LabelSelectorForMembers() string {
// 	return metav1.FormatLabelSelector(&metav1.LabelSelector{
// 		MatchLabels: a.LabelsForMember(),
// 	})
// }

// func (a *LabelPruner) findObjectsToPrune(ctx context.Context, dynamicClient dynamic.Interface, visitedUids sets.Set[types.UID], namespace string, mapping *meta.RESTMapping) ([]PruneObject, error) {
// 	applysetLabelSelector := a.LabelSelectorForMembers()

// 	opt := metav1.ListOptions{
// 		LabelSelector: applysetLabelSelector,
// 	}

// 	klog.V(2).Infof("listing objects for pruning; namespace=%q, resource=%v", namespace, mapping.Resource)
// 	objects, err := dynamicClient.Resource(mapping.Resource).Namespace(namespace).List(ctx, opt)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var pruneObjects []PruneObject
// 	for i := range objects.Items {
// 		obj := &objects.Items[i]

// 		uid := obj.GetUID()
// 		if visitedUids.Has(uid) {
// 			continue
// 		}
// 		name := obj.GetName()
// 		pruneObjects = append(pruneObjects, PruneObject{
// 			Name:      name,
// 			Namespace: namespace,
// 			Mapping:   mapping,
// 			Object:    obj,
// 		})

// 	}
// 	return pruneObjects, nil
// }

// func (a *LabelPruner) DeleteObjects(ctx context.Context, dynamicClient dynamic.Interface, pruneObjects []PruneObject, opt *ApplySetDeleteOptions) error {
// 	for i := range pruneObjects {
// 		pruneObject := &pruneObjects[i]

// 		name := pruneObject.Name
// 		namespace := pruneObject.Namespace
// 		mapping := pruneObject.Mapping

// 		if opt.DryRunStrategy != cmdutil.DryRunClient {
// 			if err := runDelete(ctx, namespace, name, mapping, dynamicClient, opt.CascadingStrategy, opt.GracePeriod, opt.DryRunStrategy == cmdutil.DryRunServer); err != nil {
// 				return fmt.Errorf("pruning %v: %w", pruneObject.String(), err)
// 			}
// 		}

// 		// opt.Printer.PrintObj(pruneObject.Object, opt.IOStreams.Out)
// 	}
// 	return nil
// }

// func runDelete(ctx context.Context, namespace, name string, mapping *meta.RESTMapping, c dynamic.Interface, cascadingStrategy metav1.DeletionPropagation, gracePeriod int, serverDryRun bool) error {
// 	options := asDeleteOptions(cascadingStrategy, gracePeriod)
// 	if serverDryRun {
// 		options.DryRun = []string{metav1.DryRunAll}
// 	}
// 	return c.Resource(mapping.Resource).Namespace(namespace).Delete(ctx, name, options)
// }

// func asDeleteOptions(cascadingStrategy metav1.DeletionPropagation, gracePeriod int) metav1.DeleteOptions {
// 	options := metav1.DeleteOptions{}
// 	if gracePeriod >= 0 {
// 		options = *metav1.NewDeleteOptions(int64(gracePeriod))
// 	}
// 	options.PropagationPolicy = &cascadingStrategy
// 	return options
// }

// // +
// // +// deleteAndReturn deletes the object and returns the value returned from the delete.
// // +// If the object was immediately deleted, the value will be nil.
// // +// If the object was not immediately deleted (e.g. due to finalizers), the value will be the object.
// // +//
// // +// This actually isn't implemented by client-go, so we fake it (for now) using an immediate GET call.
// // +// TODO: Fix this properly; delete should return the object (need a DeleteInto method?)
// // +func (p *Pruner) deleteAndReturn(ctx context.Context, gvr schema.GroupVersionResource, namespace string, name string) (*unstructured.Unstructured, error) {
// // +       var resource dynamic.ResourceInterface
// // +       if namespace != "" {
// // +               resource = p.client.Resource(gvr).Namespace(namespace)
// // +       } else {
// // +               resource = p.client.Resource(gvr)
// // +       }
// // +
// // +       if err := resource.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
// // +               return nil, err
// // +       }
// // +
// // +       afterDelete, err := resource.Get(ctx, name, metav1.GetOptions{})
// // +       if err != nil {
// // +               if apierrors.IsNotFound(err) {
// // +                       return nil, nil
// // +               } else {
// // +                       return nil, err
// // +               }
// // +       }
// // +
// // +       return afterDelete, nil
// // +}

// // +func (p *Pruner) AfterSuccesfulApply(ctx context.Context, applySet *applyset.ApplySet) (*PruneResult, error) {
// // 	+       result := &PruneResult{}
// // 	+
// // 	+       inventory := &Inventory{}
// // 	+
// // 	+       // TODO: dynamic client should have a GetInto method
// // 	+       if inventoryObj, err := p.client.Resource(inventoryGVR).Namespace(p.inventoryKey.Namespace).Get(ctx, p.inventoryKey.Name, metav1.GetOptions{}); err != nil {
// // 	+               // Should exist, because we should have created it previously
// // 	+               return nil, fmt.Errorf("failed to get inventory %s: %w", p.inventoryKey, err)
// // 	+       } else if err := runtime.DefaultUnstructuredConverter.FromUnstructured(inventoryObj.Object, inventory); err != nil {
// // 	+               return nil, fmt.Errorf("failed to parse unstructured: %w", err)
// // 	+       }
// // 	+
// // 	+       expected := make(map[InventorySpecResource]bool)
// // 	+       applySet.ForEach(func(obj applyset.ObjectInfo) {
// // 	+               gvknn := obj.GVKNN()
// // 	+               isr := buildInventorySpecResource(gvknn)
// // 	+               expected[isr] = true
// // 	+       })
// // 	+
// // 	+       // Note: we have one deleteTracker per inventory.Spec.Resources, and they are aligned
// // 	+       var deleteObjects []deleteTracker
// // 	+
// // 	+       for i := range inventory.Spec.Resources {
// // 	+               isr := inventory.Spec.Resources[i]
// // 	+               if expected[isr] {
// // 	+                       result.stateIsInUse(isr)
// // 	+                       continue
// // 	+               }
// // 	+
// // 	+               // This is an extra object ... find the GVKNN
// // 	+               gk := schema.GroupKind{
// // 	+                       Group: isr.Group,
// // 	+                       Kind:  isr.Kind,
// // 	+               }
// // 	+
// // 	+               restMapping, err := p.restMapper.RESTMapping(gk)
// // 	+               if err != nil {
// // 	+                       if meta.IsNoMatchError(err) {
// // 	+                               // The resource doesn't exist, so there can be no instances of it
// // 	+                               klog.Infof("resource %v does not exist (%v), treating object %v as deleted", gk, err, isr)
// // 	+                               deleteObjects = append(deleteObjects, deleteTracker{
// // 	+                                       removeFromInventory: true,
// // 	+                               })
// // 	+
// // 	+                               result.deleteCallSkipped(isr, err)
// // 	+                               result.stateIsDeleted(isr)
// // 	+
// // 	+                               continue
// // 	+                       } else {
// // 	+                               return nil, fmt.Errorf("error finding schema information for %v: %w", gk, err)
// // 	+                       }
// // 	+               }
// // 	+               gvknn := statewatcher.GVKNN{
// // 		+                       GVK: gk.WithVersion(restMapping.Resource.Version),
// // 		+                       NN: types.NamespacedName{
// // 		+                               Namespace: isr.Namespace,
// // 		+                               Name:      isr.Name,
// // 		+                       },
// // 		+               }
// // 		+
// // 		+               deleteObjects = append(deleteObjects, deleteTracker{
// // 		+                       gvknn: gvknn,
// // 		+                       gvr:   restMapping.Resource,
// // 		+               })
// // 		+       }
// // 		+
// // 		+       // Watch the objects we are pruning
// // 		+       {
// // 		+               var pruneWatches []statewatcher.GVKNN
// // 		+               for _, deleteObject := range deleteObjects {
// // 		+                       if deleteObject.removeFromInventory {
// // 		+                               continue
// // 		+                       }
// // 		+
// // 		+                       pruneWatches = append(pruneWatches, deleteObject.gvknn)
// // 		+               }
// // 		+               p.pruneWatches.ReplaceAllObjects(pruneWatches)
// // 		+       }
// // 		+
// // 		+       for i, deleteObject := range deleteObjects {
// // 			+               if deleteObject.removeFromInventory {
// // 			+                       // Ignore, we already know we can remove this object
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               isr := inventory.Spec.Resources[i]
// // 			+
// // 			+               // Optimization - if we're deleting it already (and waiting for the finalizers), we can skip the delete operation.
// // 			+               state, found := p.pruneWatches.ObservedState(deleteObject.gvknn)
// // 			+               if found && state.IsDeleting() {
// // 			+                       klog.Infof("resource %v was already in-progress of deletion", deleteObject.gvknn)
// // 			+                       result.deleteCallSkipped(isr, nil)
// // 			+
// // 			+                       deleteObject.waitForDeleteMinResourceVersion = state.ResourceVersion()
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               // TODO:  u := gvknn.ToUnstructured()
// // 			+               apiVersion, kind := deleteObject.gvknn.GVK.ToAPIVersionAndKind()
// // 			+               u := &unstructured.Unstructured{}
// // 			+               u.SetAPIVersion(apiVersion)
// // 			+               u.SetKind(kind)
// // 			+               u.SetNamespace(deleteObject.gvknn.NN.Namespace)
// // 			+               u.SetName(deleteObject.gvknn.NN.Name)
// // 			+
// // 			+               // A few cases here:
// // 			+               //
// // 			+               // * The object might already be deleted => detected with apierrors.IsNotFound
// // 			+               // * We might fail to delete the object (e.g. RBAC) => detected by other errors
// // 			+               // * The object might delete immediately => should be reported in watch as DELETED
// // 			+               // * The object might have finalizers => should be reported in watch as MODIFIED (deletionTimestamp non-empty); then eventually as DELETED event
// // 			+               //
// // 			+               // There is a race condition here where we don't know how far back we are lagging on the watch.
// // 			+               // It's very difficult to solve that, because delete doesn't return the resource version.
// // 			+               // Actually, the server does return the modified object in this case, however client-go discards it.
// // 			+               // We use deleteAndReturn to work-around this with an immediate GET, but this is racy.
// // 			+               // TODO: fix and remove deleteAndReturn
// // 			+
// // 			afterDelete, err := p.deleteAndReturn(ctx, deleteObject.gvr, deleteObject.gvknn.NN.Namespace, deleteObject.gvknn.NN.Name)
// // 			+               if err != nil {
// // 			+                       if apierrors.IsNotFound(err) {
// // 			+                               klog.Infof("resource %v was already deleted", deleteObject.gvknn)
// // 			+
// // 			+                               result.stateIsDeleted(isr)
// // 			+                               deleteObject.removeFromInventory = true
// // 			+
// // 			+                               continue
// // 			+                       } else {
// // 			+                               result.deleteCallDone(isr, err)
// // 			+                               result.stateIsUnknown(isr)
// // 			+                               continue
// // 			+                       }
// // 			+              } else {
// // 				+                       result.deleteCallDone(isr, nil)
// // 				+
// // 				+                       if afterDelete == nil {
// // 				+                               // Object was immediately deleted
// // 				+                               result.stateIsDeleted(isr)
// // 				+                               deleteObject.removeFromInventory = true
// // 				+                       } else {
// // 				+                               accessor, err := meta.Accessor(afterDelete)
// // 				+                               if err != nil {
// // 				+                                       // Very unexpected - this should be an object
// // 				+                                       return nil, fmt.Errorf("failed to get accessor for %T: %w", afterDelete, err)
// // 				+                               }
// // 				+
// // 				+                               rv := accessor.GetResourceVersion()
// // 				+                               deleteObject.waitForDeleteMinResourceVersion = rv
// // 				+                       }
// // 				+               }

// // 			+       }
// // 			+       // Check if any of the objects where we are waiting for delete have been deleted
// // 			+       // TODO: Should we insert a small sleep here (1 second?) if we have to wait for any objects, just so we don't always have to loop round again?
// // 			+       for i, deleteObject := range deleteObjects {
// // 			+               if deleteObject.removeFromInventory {
// // 			+                       // Ignore, we already know we can remove this object
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               if deleteObject.waitForDeleteMinResourceVersion == "" {
// // 			+                       // We're not waiting for this object
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               isr := inventory.Spec.Resources[i]
// // 			+
// // 			+               state, found := p.pruneWatches.ObservedState(deleteObject.gvknn)
// // 			+               if !found {
// // 			:
// // 			+       }

// // 			+
// // 			+       // Check if any of the objects where we are waiting for delete have been deleted
// // 			+       // TODO: Should we insert a small sleep here (1 second?) if we have to wait for any objects, just so we don't always have to loop round again?
// // 			+       for i, deleteObject := range deleteObjects {
// // 			+               if deleteObject.removeFromInventory {
// // 			+                       // Ignore, we already know we can remove this object
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               if deleteObject.waitForDeleteMinResourceVersion == "" {
// // 			+                       // We're not waiting for this object
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               isr := inventory.Spec.Resources[i]
// // 			+
// // 			+               state, found := p.pruneWatches.ObservedState(deleteObject.gvknn)
// // 			+               if !found {
// // 			+                       // This is unexpected; it indicates our watch is lagging
// // 			+                       klog.Infof("no observed state for %v", deleteObject.gvknn)
// // 			+                       result.stateIsWaitingForDeletion(isr)
// // 			+                       continue
// // 			+               }
// // 			+
// // 			+               // Make sure our watch has "caught up"
// // 			+               waitForDeleteMinResourceVersionInt, err := strconv.ParseInt(deleteObject.waitForDeleteMinResourceVersion, 10, 64)
// // 			+               if err != nil {
// // 			+                       klog.Warningf("cannot parse resourceVersion %q as int", deleteObject.waitForDeleteMinResourceVersion)
// // 			+                       waitForDeleteMinResourceVersionInt = -1
// // 			+               }
// // 			+               stateResourceVersionInt, err := strconv.ParseInt(state.ResourceVersion(), 10, 64)
// // 			+               if err != nil {
// // 			+                       klog.Warningf("cannot parse resourceVersion %q as int", state.ResourceVersion())
// // 			+                       stateResourceVersionInt = -1
// // 			+               }
// // 			+               if waitForDeleteMinResourceVersionInt != -1 && stateResourceVersionInt != -1 && waitForDeleteMinResourceVersionInt > stateResourceVersionInt {
// // 			+                       // This is unexpected; it indicates our watch is lagging
// // 			+                       klog.Infof("watch has not yet caught-up for %v: %v vs %v", deleteObject.gvknn, deleteObject.waitForDeleteMinResourceVersion, state.ResourceVersion())
// // 			+                       result.stateIsWaitingForDeletion(isr)
// // 			:
// // 			+       }
