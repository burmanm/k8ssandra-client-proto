package migrate

import (
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/migrate"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	importExample = `
	# initialize Kubernetes for Cassandra migration
	%[1]s import init [<args>]

	# Use nodetool from outside $PATH
	%[1]s import init --nodetool-path=$HOME/bin/nodetool

	`
	// errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	targetVersion string
	namespace     string
	nodetoolPath  string
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
		Use:          "init [flags]",
		Short:        "initialize importing Cassandra installation to Kubernetes",
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
	fl.StringVarP(&o.nodetoolPath, "nodetool-path", "p", "", "path to nodetool executable")
	o.configFlags.AddFlags(fl)

	o.configFlags.AddFlags(cmd.Flags())
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
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *options) Run() error {
	migrator, err := migrate.NewClusterMigrator(c.namespace)
	if err != nil {
		return err
	}

	if c.nodetoolPath != "" {
		migrator.NodetoolPath = c.nodetoolPath
	}

	return migrator.InitCluster()
}
