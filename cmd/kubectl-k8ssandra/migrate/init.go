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
	importExample = `
	# initialize Kubernetes for Cassandra migration
	%[1]s import init [<args>]

	# Use nodetool from outside $PATH
	%[1]s import init --cassandra-home=$CASSANDRA_HOME

	`
	// errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	targetVersion string
	namespace     string
	nodetoolPath  string
	cassandraHome string
}

func newOptions(streams genericclioptions.IOStreams) *options {
	return &options{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewInitCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newOptions(streams)

	cmd := &cobra.Command{
		Use:           "init [flags]",
		Short:         "initialize importing Cassandra installation to Kubernetes",
		Example:       fmt.Sprintf(importExample, "kubectl k8ssandra"),
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
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error
	// if len(args) < 0 {
	// 	return errNotEnoughParameters
	// }

	// c.targetVersion = args[0]
	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	return err
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	if c.cassandraHome == "" {
		return fmt.Errorf("cassandra-home is required")
	}
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *options) Run() error {
	spinnerLiveText, _ := pterm.DefaultSpinner.Start("Gathering information for node migration...")

	spinnerLiveText.UpdateText("Creating Kubernetes client to namespace " + c.namespace)

	client, err := cassdcutil.GetClientInNamespace(c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	pterm.Success.Println("Connected to Kubernetes node")

	migrator, err := migrate.NewClusterMigrator(client, c.namespace, c.cassandraHome)
	if err != nil {
		return err
	}

	if c.nodetoolPath != "" {
		migrator.NodetoolPath = c.nodetoolPath
	}

	err = migrator.InitCluster(spinnerLiveText)
	if err != nil {
		pterm.Error.Printf("Failed to connect to local Cassandra node to fetch required information: %v", err)
		return err
	}

	configParser := migrate.NewParser(client, c.namespace, c.cassandraHome, migrator.Datacenter)
	if err != nil {
		return err
	}

	err = configParser.ParseConfigs(spinnerLiveText)
	if err != nil {
		pterm.Error.Printf("Failed to parse local Cassandra node configuration: %v", err)
		return err
	}

	pterm.Info.Println("Initialized and parsed current Cassandra configuration. You may now review configuration before proceeding with node migration")

	n, err := migrate.NewNodeMigrator(c.namespace, c.cassandraHome)
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
