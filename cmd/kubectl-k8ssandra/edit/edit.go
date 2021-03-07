package edit

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/burmanm/k8ssandra-client/pkg/editor"
	"github.com/burmanm/k8ssandra-client/pkg/helmutil"
	"github.com/burmanm/k8ssandra-client/pkg/util"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/cli"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	editExample = `
	# launch a interactive cqlsh shell on pod
	%[1]s edit <release>
`
	errNoRelease = fmt.Errorf("no target release given, could not modify settings")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace   string
	releaseName string
	cfg         *action.Configuration
}

func newOptions(streams genericclioptions.IOStreams) *options {
	return &options{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newOptions(streams)

	cmd := &cobra.Command{
		Use:          "edit [release] [flags]",
		Short:        "edit release values",
		Example:      fmt.Sprintf(editExample, "kubectl k8ssandra"),
		SilenceUsage: true,
		// Add ValidArgsFunction?
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.Complete(c, args); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			if err := o.Run(); err != nil {
				return err
			}

			return nil
		},
	}

	o.configFlags.AddFlags(cmd.Flags())

	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error

	if len(args) < 1 {
		return errNoRelease
	}

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	actionConfig := new(action.Configuration)
	settings := cli.New()

	helmDriver := os.Getenv("HELM_DRIVER")
	if err := actionConfig.Init(settings.RESTClientGetter(), settings.Namespace(), helmDriver, func(format string, v ...interface{}) {}); err != nil {
		log.Fatal(err)
	}

	c.cfg = actionConfig
	c.releaseName = args[0]

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	return nil
}

// Run starts an interactive cqlsh shell on target pod
func (c *options) Run() error {
	// Steps, implement in pkg later - we need this for install process also
	// Fetch release's chart version
	// TODO We could do this in Validate also..
	targetVersion, err := helmutil.ChartVersion("")
	if err != nil {
		return err
	}
	// Verify if we have local copy of the values.yaml
	// 	if not, fetch it
	chartDir, err := helmutil.DownloadChartRelease(targetVersion)
	if err != nil {
		return err
	}

	// Create temp file with merged default values.yaml (with comments) and helm modified values
	// If there were changes, upgrade Helm release with the new overridden settings

	targetFilename := filepath.Join(chartDir, helmutil.ChartName, "values.yaml")

	// TODO Following does not belong here.. move to some pkg

	file, err := os.Open(targetFilename)
	if err != nil {
		return err
	}

	yamlInput, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	defer file.Close()

	// TODO Perhaps to the helm-cache directory instead?
	cacheDir, err := util.GetCacheDir("helm")
	if err != nil {
		return err
	}

	// TODO TempFile / outputFile should have correct .yaml ending for the editor theme
	outputFile, err := ioutil.TempFile(cacheDir, "*.yaml")
	if err != nil {
		return err
	}

	defer outputFile.Close()

	var value yaml.Node
	err = yaml.Unmarshal(yamlInput, &value)
	if err != nil {
		return err
	}

	// Fetch Helm values
	values, err := helmutil.SetValues(c.cfg, c.releaseName)
	if err != nil {
		return err
	}

	encodeStep, err := yaml.Marshal(values)
	if err != nil {
		return err
	}

	var overrides yaml.Node
	err = yaml.Unmarshal(encodeStep, &overrides)
	if err != nil {
		return err
	}

	err = recursiveMerge(&overrides, &value)
	if err != nil {
		return err
	}

	// TODO Merge value changes to yaml.Node above

	writtenYaml, err := yaml.Marshal(&value)
	if err != nil {
		return err
	}

	_, err = outputFile.Write(writtenYaml)
	if err != nil {
		return err
	}

	// return nil
	return editor.OpenEditor(outputFile.Name())

	// TODO Should we only add values that were modified? In other words, opposite of merge?

	// TODO Remove the temp file after helm upgrade has finished?
}

// Following code modified from https://stackoverflow.com/questions/65768861/read-and-merge-two-yaml-files-dynamically-and-or-recursively
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
				// fmt.Printf("Adding: %v\n", from.Content[i])
				// Should not happen if the values are already in there..

				// FIX This creates ugly format for our current implementation: heap: {size: 800M} instead of heap:\nsize:..

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
		into.Value = from.Value
	default:
		return errors.New("can only merge mapping, scalar and sequence nodes")
	}
	return nil
}
