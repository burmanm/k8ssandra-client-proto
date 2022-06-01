package edit

import (
	"fmt"
	"log"
	"os"

	"github.com/burmanm/k8ssandra-client/pkg/editor"
	"github.com/burmanm/k8ssandra-client/pkg/helmutil"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/cli"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	editExample = `
	# edit configuration of a release (installation of k8ssandra)
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
	settings    *cli.EnvSettings
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

	c.settings = settings
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
	// TODO We need the version also
	// TODO We could do this in Validate also..

	// TODO Implement ChartVersion
	chartInfo, err := helmutil.ChartVersion(c.cfg, c.releaseName)
	if err != nil {
		return err
	}

	// Verify if we have local copy of the values.yaml
	// 	if not, fetch it
	chartDir, err := helmutil.DownloadChartRelease(chartInfo.Metadata.Name, chartInfo.Metadata.Version)
	if err != nil {
		return err
	}

	chatExtractDir, err := helmutil.ExtractChartRelease(chartDir, chartInfo.Metadata.Version)
	if err != nil {
		return err
	}

	outputFile, err := helmutil.MergeValuesFile(c.cfg, c.settings, chatExtractDir, chartInfo.Metadata.Version, chartInfo.Metadata.Name, c.releaseName)
	if err != nil {
		return err
	}

	err = editor.OpenEditor(outputFile.Name())
	if err != nil {
		outputFile.Close()
		return err
	}

	outputFile.Close()

	file, err := os.Open(outputFile.Name())
	if err != nil {
		return err
	}

	_, err = helmutil.UpgradeValues(c.cfg, chatExtractDir, chartInfo.Metadata.Name, c.releaseName, file)
	return err

	// TODO Should we only add values that were modified? In other words, opposite of merge?
	// TODO Remove the temp file after helm upgrade has finished?
}
