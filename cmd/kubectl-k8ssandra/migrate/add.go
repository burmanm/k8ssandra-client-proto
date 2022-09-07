package migrate

import (
	"context"
	"fmt"
	"sync"

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

	# Override DSE_HOME env variable
	%[1]s import add --cassandra-home=/path/to/dse/installation

	# Override configuration paths
	%[1]s import add --cass-config-dir=/usr/local/dse/cassandra/ --dse-config-dir=/usr/local/dse/

	# Override nodetool location
	%[1]s import add --nodetool-path=/usr/bin/nodetool
	`
	errNoCassandraHome = fmt.Errorf("cassandra-home was not detected")
)

type addOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace     string
	nodetoolPath  string
	cassandraHome string
	dseConfigDir  string
	cassConfigDir string
	configDir     string
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
	fl.StringVarP(&o.nodetoolPath, "nodetool-path", "p", "", "path to override nodetool executable path")
	fl.StringVarP(&o.cassandraHome, "cassandra-home", "c", "", "path to override cassandra/DSE installation directory")
	fl.StringVarP(&o.cassConfigDir, "cass-config-dir", "c", "", "override cassandra.yaml configuration directory")
	fl.StringVarP(&o.dseConfigDir, "dse-config-dir", "c", "", "override dse.yaml configuration directory")
	fl.StringVarP(&o.configDir, "config-dir", "f", "", "path to cassandra/DSE configuration directory")
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

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *addOptions) Validate() error {
	cassandraHome, nodetoolPath, err := migrate.DetectInstallation(c.cassandraHome, c.nodetoolPath)
	if err != nil {
		return err
	}
	c.cassandraHome = cassandraHome
	c.nodetoolPath = nodetoolPath
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *addOptions) Run() error {
	p, _ := pterm.DefaultSpinner.Start("Gathering information for node migration...")

	p.UpdateText("Creating Kubernetes client to namespace " + c.namespace)

	restConfig, err := c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	kubeClient, err := cassdcutil.GetClientInNamespace(restConfig, c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	pterm.Success.Println("Connected to Kubernetes node")

	// TODO This logic belongs to the pkg

	lock, err := migrate.NewResourceLock(c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to create resource lock: %v", err)
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	p.UpdateText("Waiting for migrator lock")

	// Gain leader election and then proceed
	go migrate.RunLeaderElection(ctx, wg, lock)
	wg.Wait()

	pterm.Success.Println("Acquired node migrator lock")

	n := migrate.NewNodeMigrator(kubeClient, c.namespace)
	if err != nil {
		return err
	}

	err = n.MigrateNode(p)
	if err != nil {
		pterm.Error.Printf("Failed to migrate local Cassandra node to Kubernetes: %v", err)
		return err
	}

	p.Success("Cassandra node has been successfully migrated to Kubernetes")

	return nil
}
