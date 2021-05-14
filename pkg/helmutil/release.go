package helmutil

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/burmanm/k8ssandra-client/pkg/util"
	"gopkg.in/yaml.v3"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/release"
)

// ChartVersion gets the release's chart version or returns an error if it did not exist
func ChartVersion(cfg *action.Configuration, releaseName string) (string, error) {
	rel, err := Release(cfg, releaseName)
	if err != nil {
		return "", err
	}

	return rel.Chart.Metadata.Version, nil
}

// SetValues returns the deployed Helm releases modified values
func SetValues(cfg *action.Configuration, releaseName string) (map[string]interface{}, error) {
	client := action.NewGetValues(cfg)
	return client.Run(releaseName)
}

func UpgradeValues(cfg *action.Configuration, chartDir, releaseName string, inputValues *os.File) (*release.Release, error) {
	u := action.NewUpgrade(cfg)
	u.ReuseValues = true

	// Needs the chartPath we just Merged values from ..

	// Check chart dependencies to make sure all are present in /charts
	chartDir = filepath.Join(chartDir, ChartName)
	ch, err := loader.Load(chartDir)
	if err != nil {
		return nil, err
	}
	if req := ch.Metadata.Dependencies; req != nil {
		if err := action.CheckDependencies(ch, req); err != nil {
			// TODO We should autodownload these
			return nil, err
		}
	}
	// Read the input file as values
	data, err := ioutil.ReadAll(inputValues)
	if err != nil {
		return nil, err
	}

	var values map[string]interface{}
	err = yaml.Unmarshal(data, &values)
	if err != nil {
		return nil, err
	}

	// Needs chart and vals
	return u.Run(releaseName, ch, values)
}

func MergeValuesFile(cfg *action.Configuration, settings *cli.EnvSettings, chartDir, targetVersion, releaseName string) (*os.File, error) {
	// Create temp file with merged default values.yaml (with comments) and helm modified values
	// If there were changes, upgrade Helm release with the new overridden settings

	targetFilename := filepath.Join(chartDir, ChartName, "values.yaml")

	// TODO Following does not belong here.. move to some pkg

	file, err := os.Open(targetFilename)
	if err != nil {
		return nil, err
	}

	yamlInput, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	cacheDir, err := util.GetCacheDir("helm")
	if err != nil {
		return nil, err
	}

	outputFile, err := ioutil.TempFile(cacheDir, "*.yaml")
	if err != nil {
		return nil, err
	}

	// defer outputFile.Close()

	var value yaml.Node
	err = yaml.Unmarshal(yamlInput, &value)
	if err != nil {
		return nil, err
	}

	// Fetch Helm values
	values, err := SetValues(cfg, releaseName)
	if err != nil {
		return nil, err
	}

	encodeStep, err := yaml.Marshal(values)
	if err != nil {
		return nil, err
	}

	var overrides yaml.Node
	err = yaml.Unmarshal(encodeStep, &overrides)
	if err != nil {
		return nil, err
	}

	err = recursiveMerge(&overrides, &value)
	if err != nil {
		return nil, err
	}

	writtenYaml, err := yaml.Marshal(&value)
	if err != nil {
		return nil, err
	}

	_, err = outputFile.Write(writtenYaml)
	if err != nil {
		return nil, err
	}

	return outputFile, err
}

func nodesEqual(l, r *yaml.Node) bool {
	if l.Kind == yaml.ScalarNode && r.Kind == yaml.ScalarNode {
		return l.Value == r.Value
	}
	panic("anchors are not supported by the k8ssandra values")
}

func recursiveMerge(from, into *yaml.Node) error {
	if from.Kind != into.Kind {
		return errors.New("Unable to merge input values")
	}
	switch from.Kind {
	case yaml.MappingNode:
		for i := 0; i < len(from.Content); i += 2 {
			found := false
			for j := 0; j < len(into.Content); j += 2 {
				if nodesEqual(from.Content[i], into.Content[j]) {
					found = true
					if err := recursiveMerge(from.Content[i+1], into.Content[j+1]); err != nil {
						return errors.New("Failed to parse input key " + from.Content[i].Value + ": " + err.Error())
					}
					break
				}
			}
			if !found {
				// TODO This creates ugly format for our current implementation: heap: {size: 800M} instead of heap:\nsize:..
				// TODO Test that we add some values to the map, but do not duplicate any
				into.Content = append(into.Content, from.Content[i:i+2]...)
			}
		}
	case yaml.SequenceNode:
	IntoAdd:
		for _, v := range from.Content {
			for _, existing := range into.Content {
				if existing.Value == v.Value {
					continue IntoAdd
				}
			}
			into.Content = append(into.Content, v)
		}
	case yaml.DocumentNode:
		recursiveMerge(from.Content[0], into.Content[0])
	case yaml.ScalarNode:
		if from.Tag == "!!float" && into.Tag == "!!int" {
			// We need a marshalling trick to get it correctly back to int
			out, err := yaml.Marshal(from)
			if err != nil {
				return err
			}
			var newVal int
			yaml.Unmarshal(out, &newVal)
			into.Value = strconv.Itoa(newVal)
		} else {
			into.Value = from.Value
		}
	default:
		return errors.New("can only merge mapping, scalar and sequence nodes")
	}
	return nil
}
