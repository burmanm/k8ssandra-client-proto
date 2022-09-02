package helmutil

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/burmanm/k8ssandra-client/pkg/util"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/downloader"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/repo"
)

// DownloadChartRelease fetches the k8ssandra target version and extracts it to a directory which path is returned
func DownloadChartRelease(repoName, repoURL, chartName, targetVersion string) (string, error) {
	// Unfortunately, the helm's chart pull command uses "internal" marked structs, so it can't be used for
	// pulling the data. Thus, we need to replicate the implementation here and use our own cache
	settings := cli.New()
	var out strings.Builder

	c := downloader.ChartDownloader{
		Out: &out,
		// Keyring: p.Keyring,
		Verify:  downloader.VerifyNever,
		Getters: getter.All(settings),
		Options: []getter.Option{
			// getter.WithBasicAuth(p.Username, p.Password),
			// getter.WithTLSClientConfig(p.CertFile, p.KeyFile, p.CaFile),
			// getter.WithInsecureSkipVerifyTLS(p.InsecureSkipTLSverify),
		},
		RepositoryConfig: settings.RepositoryConfig,
		RepositoryCache:  settings.RepositoryCache,
	}

	// helm repo add k8ssandra https://helm.k8ssandra.io/
	r, err := repo.NewChartRepository(&repo.Entry{
		Name: repoName,
		URL:  repoURL,
	}, getter.All(settings))

	if err != nil {
		return "", err
	}

	// helm repo update k8ssandra
	index, err := r.DownloadIndexFile()
	if err != nil {
		return "", err
	}

	// Read the index file for the repository to get chart information and return chart URL
	repoIndex, err := repo.LoadIndexFile(index)
	if err != nil {
		return "", err
	}

	// chart name, chart version
	cv, err := repoIndex.Get(chartName, targetVersion)
	if err != nil {
		return "", err
	}

	url, err := repo.ResolveReferenceURL(repoURL, cv.URLs[0])
	if err != nil {
		return "", err
	}

	// Download to filesystem for extraction purposes
	dir, err := os.MkdirTemp("", "helmutil-")
	if err != nil {
		return "", err
	}

	// TODO We can't do removeAll here..
	// defer os.RemoveAll(dir)

	// _ is ProvenanceVerify (TODO we might want to verify the release)
	saved, _, err := c.DownloadTo(url, targetVersion, dir)
	if err != nil {
		return "", err
	}

	return saved, nil
}

func ExtractChartRelease(saved, targetVersion string) (string, error) {
	// TODO We need saved for the install process, clip from here to another function..

	// Extract the files
	subDir := filepath.Join("helm", targetVersion)
	extractDir, err := util.GetCacheDir(subDir)
	if err != nil {
		return "", err
	}

	// extractDir is a target directory
	err = chartutil.ExpandFile(extractDir, saved)
	if err != nil {
		return "", err
	}

	return extractDir, nil
}

func Release(cfg *action.Configuration, releaseName string) (*release.Release, error) {
	getAction := action.NewGet(cfg)
	return getAction.Run(releaseName)
}

func ListInstallations(cfg *action.Configuration) ([]*release.Release, error) {
	listAction := action.NewList(cfg)
	listAction.AllNamespaces = true
	return listAction.Run()
}

func Install(cfg *action.Configuration, releaseName, path, namespace string, values map[string]interface{}) (*release.Release, error) {
	installAction := action.NewInstall(cfg)
	installAction.ReleaseName = releaseName
	installAction.Namespace = namespace
	if releaseName == "mc" {
		installAction.Devel = true
		installAction.Version = ">0.0.0.0"
	}
	chartReq, err := loader.Load(path)
	if err != nil {
		return nil, err
	}

	return installAction.Run(chartReq, values)
}

func Uninstall(cfg *action.Configuration, releaseName string) (*release.UninstallReleaseResponse, error) {
	uninstallAction := action.NewUninstall(cfg)
	return uninstallAction.Run(releaseName)
}

// ValuesYaml fetches the chartVersion's values.yaml file for editing purposes
func ValuesYaml(targetVersion string) (io.Reader, error) {
	return nil, nil
}
