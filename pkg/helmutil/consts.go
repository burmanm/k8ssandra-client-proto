package helmutil

const (
	ManagedLabel      = "app.kubernetes.io/managed-by"
	ManagedLabelValue = "Helm"
	ReleaseAnnotation = "meta.helm.sh/release-name"
)
