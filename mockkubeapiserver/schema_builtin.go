package mockkubeapiserver

import "k8s.io/apimachinery/pkg/runtime/schema"

func buildTypeInfo(gvk schema.GroupVersionKind) typeInfo {
	var i typeInfo
	i.Name = gvk.Group + "." + gvk.Kind
	i.Properties = map[string]propertyInfo{}

	// TODO: Make objectMetaTypeInfo shared if there are no absolute paths in it
	var objectMetaTypeInfo typeInfo
	objectMetaTypeInfo.Name = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
	objectMetaTypeInfo.Properties = map[string]propertyInfo{}
	objectMetaTypeInfo.Properties["ownerReferences"] = propertyInfo{
		Key:           "ownerReferences",
		MergeKey:      "uid",
		PatchStrategy: "merge",
	}

	i.Properties["metadata"] = propertyInfo{Key: "metadata", Type: &objectMetaTypeInfo}

	return i
}
