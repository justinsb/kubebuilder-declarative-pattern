package prune

// import (
// 	"crypto/sha256"
// 	"encoding/base64"
// 	"fmt"
// 	"strings"

// 	"k8s.io/apimachinery/pkg/api/errors"
// 	"k8s.io/apimachinery/pkg/api/meta"
// 	"k8s.io/apimachinery/pkg/runtime/schema"
// 	"k8s.io/apimachinery/pkg/util/sets"
// 	"k8s.io/cli-runtime/pkg/resource"
// )

// // Label and annotation keys from the ApplySet specification.
// // https://git.k8s.io/enhancements/keps/sig-cli/3659-kubectl-apply-prune#design-details-applyset-specification
// const (
// 	// ApplySetToolingAnnotation is the key of the label that indicates which tool is used to manage this ApplySet.
// 	// Tooling should refuse to mutate ApplySets belonging to other tools.
// 	// The value must be in the format <toolname>/<semver>.
// 	// Example value: "kubectl/v1.27" or "helm/v3" or "kpt/v1.0.0"
// 	ApplySetToolingAnnotation = "applyset.kubernetes.io/tooling"

// 	// ApplySetAdditionalNamespacesAnnotation annotation extends the scope of the ApplySet beyond the parent
// 	// object's own namespace (if any) to include the listed namespaces. The value is a comma-separated
// 	// list of the names of namespaces other than the parent's namespace in which objects are found
// 	// Example value: "kube-system,ns1,ns2".
// 	ApplySetAdditionalNamespacesAnnotation = "applyset.kubernetes.io/additional-namespaces"

// 	// ApplySetGRsAnnotation is a list of group-resources used to optimize listing of ApplySet member objects.
// 	// It is optional in the ApplySet specification, as tools can perform discovery or use a different optimization.
// 	// However, it is currently required in kubectl.
// 	// When present, the value of this annotation must be a comma separated list of the group-kinds,
// 	// in the fully-qualified name format, i.e. <resourcename>.<group>.
// 	// Example value: "certificates.cert-manager.io,configmaps,deployments.apps,secrets,services"
// 	ApplySetGRsAnnotation = "applyset.kubernetes.io/contains-group-resources"

// 	// ApplySetParentIDLabel is the key of the label that makes object an ApplySet parent object.
// 	// Its value MUST use the format specified in V1ApplySetIdFormat below
// 	ApplySetParentIDLabel = "applyset.kubernetes.io/id"

// 	// V1ApplySetIdFormat is the format required for the value of ApplySetParentIDLabel (and ApplysetPartOfLabel).
// 	// The %s segment is the unique ID of the object itself, which MUST be the base64 encoding
// 	// (using the URL safe encoding of RFC4648) of the hash of the GKNN of the object it is on, in the form:
// 	// base64(sha256(<name>.<namespace>.<kind>.<group>)).
// 	V1ApplySetIdFormat = "applyset-%s-v1"

// 	// ApplysetPartOfLabel is the key of the label which indicates that the object is a member of an ApplySet.
// 	// The value of the label MUST match the value of ApplySetParentIDLabel on the parent object.
// 	ApplysetPartOfLabel = "applyset.kubernetes.io/part-of"

// 	// ApplysetParentCRDLabel is the key of the label that can be set on a CRD to identify
// 	// the custom resource type it defines (not the CRD itself) as an allowed parent for an ApplySet.
// 	ApplysetParentCRDLabel = "applyset.kubernetes.io/is-parent-type"
// )

// type ParentManager struct {
// }

// func (m *ParentManager) PrunerForObject() *ParentPruner {

// }

// type ParentPruner struct {

// 	// parentRef is a reference to the parent object that is used to track the applyset.
// 	parentRef *ApplySetParentRef

// 	currentResources  []*meta.RESTMapping
// 	currentNamespaces map[string]bool
// }

// // ApplySetParentRef stores object and type meta for the parent object that is used to track the applyset.
// type ApplySetParentRef struct {
// 	Name      string
// 	Namespace string
// 	// *meta.RESTMapping
// }

// // func (p ApplySetParentRef) IsNamespaced() bool {
// // 	return p.Scope.Name() == meta.RESTScopeNameNamespace
// // }

// const applySetIDPartDelimiter = "."

// // ID is the label value that we are using to identify this applyset.
// // Format: base64(sha256(<name>.<namespace>.<kind>.<group>)), using the URL safe encoding of RFC4648.

// func (p *ParentPruner) ID() string {
// 	unencoded := strings.Join([]string{a.parentRef.Name, a.parentRef.Namespace, a.parentRef.GroupVersionKind.Kind, a.parentRef.GroupVersionKind.Group}, applySetIDPartDelimiter)
// 	hashed := sha256.Sum256([]byte(unencoded))
// 	b64 := base64.RawURLEncoding.EncodeToString(hashed[:])
// 	// Label values must start and end with alphanumeric values, so add a known-safe prefix and suffix.
// 	return fmt.Sprintf(V1ApplySetIdFormat, b64)
// }

// func (a *ParentPruner) LabelsForMember() map[string]string {
// 	return map[string]string{
// 		ApplysetPartOfLabel: a.ID(),
// 	}
// }

// // AllPrunableResources returns the list of all resources that should be considered for pruning.
// // This is potentially a superset of the resources types that actually contain resources.
// func (a *ParentPruner) AllPrunableResources() []*meta.RESTMapping {
// 	var ret []*meta.RESTMapping
// 	for _, m := range a.currentResources {
// 		ret = append(ret, m)
// 	}
// 	return ret
// }

// // AllPrunableNamespaces returns the list of all namespaces that should be considered for pruning.
// // This is potentially a superset of the namespaces that actually contain resources.
// func (a *ParentPruner) AllPrunableNamespaces() []string {
// 	var ret []string
// 	for ns := range a.currentNamespaces {
// 		ret = append(ret, ns)
// 	}
// 	return ret
// }

// // addLabels sets our tracking labels on each object; this should be called as part of loading the objects.
// func (a *ParentPruner) AddLabels(objects ...*resource.Info) error {
// 	applysetLabels := a.LabelsForMember()
// 	for _, obj := range objects {
// 		accessor, err := meta.Accessor(obj.Object)
// 		if err != nil {
// 			return fmt.Errorf("getting accessor: %w", err)
// 		}
// 		labels := accessor.GetLabels()
// 		if labels == nil {
// 			labels = make(map[string]string)
// 		}
// 		for k, v := range applysetLabels {
// 			if _, found := labels[k]; found {
// 				return fmt.Errorf("ApplySet label %q already set in input data", k)
// 			}
// 			labels[k] = v
// 		}
// 		accessor.SetLabels(labels)
// 	}

// 	return nil
// }

// func (a *ParentPruner) fetchParent() error {
// 	helper := resource.NewHelper(a.client, a.parentRef.RESTMapping)
// 	obj, err := helper.Get(a.parentRef.Namespace, a.parentRef.Name)
// 	if errors.IsNotFound(err) {
// 		if !builtinApplySetParentGVRs.Has(a.parentRef.Resource) {
// 			return fmt.Errorf("custom resource ApplySet parents cannot be created automatically")
// 		}
// 		return nil
// 	} else if err != nil {
// 		return fmt.Errorf("failed to fetch ApplySet parent object %q: %w", a.parentRef, err)
// 	} else if obj == nil {
// 		return fmt.Errorf("failed to fetch ApplySet parent object %q", a.parentRef)
// 	}

// 	labels, annotations, err := getLabelsAndAnnotations(obj)
// 	if err != nil {
// 		return fmt.Errorf("getting metadata from parent object %q: %w", a.parentRef, err)
// 	}

// 	toolAnnotation, hasToolAnno := annotations[ApplySetToolingAnnotation]
// 	if !hasToolAnno {
// 		return fmt.Errorf("ApplySet parent object %q already exists and is missing required annotation %q", a.parentRef, ApplySetToolingAnnotation)
// 	}
// 	if managedBy := toolingBaseName(toolAnnotation); managedBy != a.toolingID.Name {
// 		return fmt.Errorf("ApplySet parent object %q already exists and is managed by tooling %q instead of %q", a.parentRef, managedBy, a.toolingID.Name)
// 	}

// 	idLabel, hasIDLabel := labels[ApplySetParentIDLabel]
// 	if !hasIDLabel {
// 		return fmt.Errorf("ApplySet parent object %q exists and does not have required label %s", a.parentRef, ApplySetParentIDLabel)
// 	}
// 	if idLabel != a.ID() {
// 		return fmt.Errorf("ApplySet parent object %q exists and has incorrect value for label %q (got: %s, want: %s)", a.parentRef, ApplySetParentIDLabel, idLabel, a.ID())
// 	}

// 	if a.currentResources, err = parseResourcesAnnotation(annotations, a.restMapper); err != nil {
// 		// TODO: handle GVRs for now-deleted CRDs
// 		return fmt.Errorf("parsing ApplySet annotation on %q: %w", a.parentRef, err)
// 	}
// 	a.currentNamespaces = parseNamespacesAnnotation(annotations)
// 	if a.parentRef.IsNamespaced() {
// 		a.currentNamespaces.Insert(a.parentRef.Namespace)
// 	}
// 	return nil
// }

// func parseResourcesAnnotation(annotations map[string]string, mapper meta.RESTMapper) (map[schema.GroupVersionResource]*meta.RESTMapping, error) {
// 	annotation, ok := annotations[ApplySetGRsAnnotation]
// 	if !ok {
// 		// The spec does not require this annotation. However, 'missing' means 'perform discovery'.
// 		// We return an error because we do not currently support dynamic discovery in kubectl apply.
// 		return nil, fmt.Errorf("kubectl requires the %q annotation to be set on all ApplySet parent objects", ApplySetGRsAnnotation)
// 	}
// 	mappings := make(map[schema.GroupVersionResource]*meta.RESTMapping)
// 	// Annotation present but empty means that this is currently an empty set.
// 	if annotation == "" {
// 		return mappings, nil
// 	}
// 	for _, grString := range strings.Split(annotation, ",") {
// 		gr := schema.ParseGroupResource(grString)
// 		gvk, err := mapper.KindFor(gr.WithVersion(""))
// 		if err != nil {
// 			return nil, fmt.Errorf("invalid group resource in %q annotation: %w", ApplySetGRsAnnotation, err)
// 		}
// 		mapping, err := mapper.RESTMapping(gvk.GroupKind())
// 		if err != nil {
// 			return nil, fmt.Errorf("could not find kind for resource in %q annotation: %w", ApplySetGRsAnnotation, err)
// 		}
// 		mappings[mapping.Resource] = mapping
// 	}
// 	return mappings, nil
// }

// func parseNamespacesAnnotation(annotations map[string]string) sets.Set[string] {
// 	annotation, ok := annotations[ApplySetAdditionalNamespacesAnnotation]
// 	if !ok { // this annotation is completely optional
// 		return sets.Set[string]{}
// 	}
// 	// Don't include an empty namespace
// 	if annotation == "" {
// 		return sets.Set[string]{}
// 	}
// 	return sets.New(strings.Split(annotation, ",")...)
// }
