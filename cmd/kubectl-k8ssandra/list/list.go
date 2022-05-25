package list

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/burmanm/k8ssandra-client/pkg/helmutil"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/release"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	listExample = `
	# list all installed k8ssandra clusters
	%[1]s list
`
	// errNoRelease = fmt.Errorf("no target release given, could not modify settings")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	cfg              *action.Configuration
	namespace        string
	enforceNamespace bool
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
		Use:          "list [release] [flags]",
		Short:        "list release values",
		Example:      fmt.Sprintf(listExample, "kubectl k8ssandra"),
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
	namespace, enforceNamespace, err := c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	actionConfig := new(action.Configuration)
	settings := cli.New()

	helmDriver := os.Getenv("HELM_DRIVER")
	helmNamespace := ""
	if enforceNamespace {
		helmNamespace = namespace
	}
	if err := actionConfig.Init(settings.RESTClientGetter(), helmNamespace, helmDriver, func(format string, v ...interface{}) {}); err != nil {
		log.Fatal(err)
	}

	c.cfg = actionConfig

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	return nil
}

// Run starts an interactive cqlsh shell on target pod
func (c *options) Run() error {
	releases, err := helmutil.ListInstallations(c.cfg)
	if err != nil {
		return err
	}
	printReleases(releases)
	return nil
}

type releaseElement struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Revision  string `json:"revision"`
	Updated   string `json:"updated"`
	// We only fetch Deployed & Failed status at the moment, filtering in fetch.go
	Status           string `json:"status"`
	ChartVersion     string `json:"chart_version"`
	CassandraVersion string `json:"cass_version"`
	Chart            string `json:"chart"`
}

func printReleases(releases []*release.Release) {
	// Initialize the array so no results returns an empty array instead of null
	elements := make([]releaseElement, 0, len(releases))
	for _, r := range releases {
		// Listing CassandraVersion requires parsing the chart + possible overridden values
		element := releaseElement{
			Name:         r.Name,
			Namespace:    r.Namespace,
			Revision:     strconv.Itoa(r.Version),
			Status:       r.Info.Status.String(),
			ChartVersion: r.Chart.Metadata.Version,
			Chart:        r.Chart.Name(),
			Updated:      r.Info.LastDeployed.Local().String(),
			// CassandraVersion: r.Chart.Values["cassandra"]["version"],
		}

		elements = append(elements, element)
	}

	// TODO Replace with nice table list - add headers first
	for _, e := range elements {
		fmt.Printf("%v\n", e)
	}
}
