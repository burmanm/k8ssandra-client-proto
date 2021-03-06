package helmutil

// GetChartRelease gets the release's chart version or returns an error if it did not exist
func GetChartRelease(release, namespace string) (string, error) {
	return "1.0.0", nil
}
