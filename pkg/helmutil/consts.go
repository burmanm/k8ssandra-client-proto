package helmutil

const (
	ManagedLabel      = "app.kubernetes.io/managed-by"
	ManagedLabelValue = "Helm"
	ReleaseAnnotation = "meta.helm.sh/release-name"

	RepoURL = "https://helm.k8ssandra.io/"
	// ChartName is the name of k8ssandra's helm repo chart
	ChartName = "k8ssandra"
)
