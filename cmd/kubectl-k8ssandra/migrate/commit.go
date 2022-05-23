package migrate

import (
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/burmanm/k8ssandra-client/pkg/migrate"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	importCommitExample = `
	# finish Cassandra to k8ssandra migration for Datacenter dc1
	%[1]s import commit dc1 [<args>]

	`
	errNoDatacenter = fmt.Errorf("datacenter parameter is required")
)

type commitOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace  string
	datacenter string
}

func newCommitOptions(streams genericclioptions.IOStreams) *commitOptions {
	return &commitOptions{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewCommitCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newCommitOptions(streams)

	cmd := &cobra.Command{
		Use:          "commit <datacenter> [flags]",
		Short:        "finish importing Cassandra installation to Kubernetes",
		Example:      fmt.Sprintf(importExample, "kubectl k8ssandra"),
		SilenceUsage: true,
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

	fl := cmd.Flags()
	o.configFlags.AddFlags(fl)
	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *commitOptions) Complete(cmd *cobra.Command, args []string) error {
	var err error

	if len(args) < 1 {
		return errNoDatacenter
	}

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	c.datacenter = args[0]

	// migrate is our default namespace
	if c.namespace == "default" || c.namespace == "" {
		c.namespace = releaseName
	}

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *commitOptions) Validate() error {
	if len(c.datacenter) == 0 {
		return errNoDatacenter
	}
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *commitOptions) Run() error {
	spinnerLiveText, _ := pterm.DefaultSpinner.Start("Gathering information for node migration...")

	spinnerLiveText.UpdateText("Creating Kubernetes client to namespace " + c.namespace)

	client, err := cassdcutil.GetClientInNamespace(c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	pterm.Success.Println("Connected to Kubernetes node")

	migrator := migrate.NewMigrateFinisher(client, c.namespace, c.datacenter)

	err = migrator.FinishInstallation(spinnerLiveText)
	if err != nil {
		pterm.Error.Printf("Failed to finish the k8ssandra installation: %v", err)
		return err
	}
	return nil
}
