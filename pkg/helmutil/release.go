package helmutil

import "helm.sh/helm/v3/pkg/action"

// ChartVersion gets the release's chart version or returns an error if it did not exist
func ChartVersion(releaseName string) (string, error) {
	// TODO We need to parse the correct chart version..
	return "1.0.0", nil
}

// SetValues returns the deployed Helm releases modified values
func SetValues(cfg *action.Configuration, releaseName string) (map[string]interface{}, error) {
	client := action.NewGetValues(cfg)
	return client.Run(releaseName)
}
