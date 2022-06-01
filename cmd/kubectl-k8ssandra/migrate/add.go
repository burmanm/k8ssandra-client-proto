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
	importAddExample = `
	# migrate local Cassandra node to the Kubernetes
	%[1]s import add [<args>]

	# Define CASSANDRA_HOME
	%[1]s import add --cassandra-home=$CASSANDRA_HOME

	`
	errNoCassandraHome = fmt.Errorf("cassandra-home is required parameter")
)

type addOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace     string
	nodetoolPath  string
	cassandraHome string
}

func newAddOptions(streams genericclioptions.IOStreams) *addOptions {
	return &addOptions{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewAddCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newAddOptions(streams)

	cmd := &cobra.Command{
		Use:           "add [flags]",
		Short:         "import local Cassandra installation to Kubernetes",
		Example:       fmt.Sprintf(importAddExample, "kubectl k8ssandra"),
		SilenceUsage:  true,
		SilenceErrors: true,
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
	fl.StringVarP(&o.nodetoolPath, "nodetool-path", "p", "", "path to nodetool executable directory")
	fl.StringVarP(&o.cassandraHome, "cassandra-home", "c", "", "path to cassandra/DSE installation directory")
	o.configFlags.AddFlags(fl)
	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *addOptions) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	// Create new namespace for this usage
	if c.namespace == "default" || c.namespace == "" {
		c.namespace = releaseName
	}

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *addOptions) Validate() error {
	if c.cassandraHome == "" {
		return errNoCassandraHome
	}
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *addOptions) Run() error {
	spinnerLiveText, _ := pterm.DefaultSpinner.Start("Gathering information for node migration...")

	spinnerLiveText.UpdateText("Creating Kubernetes client to namespace " + c.namespace)

	client, err := cassdcutil.GetClientInNamespace(c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	pterm.Success.Println("Connected to Kubernetes node")

	n := migrate.NewNodeMigrator(client, c.namespace, c.cassandraHome)
	if err != nil {
		return err
	}

	if c.nodetoolPath != "" {
		n.NodetoolPath = c.nodetoolPath
	}

	err = n.MigrateNode(spinnerLiveText)
	if err != nil {
		pterm.Error.Printf("Failed to migrate local Cassandra node to Kubernetes: %v", err)
		return err
	}

	spinnerLiveText.Success("Cassandra node has been successfully migrated to Kubernetes")

	return nil
}
