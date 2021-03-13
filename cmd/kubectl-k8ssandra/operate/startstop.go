package operate

import (
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	startExample = `
	# start an existing cluster that was stopped
	%[1]s start <cluster>
	`

	stopExample = `
	# shutdown an existing cluster
	%[1]s stop <cluster>

	# shutdown an existing cluster and wait for all the pods to shutdown
	%[1]s stop <cluster> --wait
	`

	errNoClusterDefined = fmt.Errorf("no target cluster defined, could not modify state")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace   string
	dcName      string
	wait        bool
	cassManager *cassdcutil.CassManager
}

func newOptions(streams genericclioptions.IOStreams) *options {
	return &options{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

func NewStartCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newOptions(streams)

	cmd := &cobra.Command{
		Use:          "start [cluster]",
		Short:        "restart an existing shutdown Cassandra cluster",
		Example:      fmt.Sprintf(startExample, "kubectl k8ssandra"),
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.Complete(c, args); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			if err := o.Run(false); err != nil {
				return err
			}

			return nil
		},
	}

	o.configFlags.AddFlags(cmd.Flags())
	return cmd
}

func NewStopCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newOptions(streams)

	cmd := &cobra.Command{
		Use:          "stop [cluster]",
		Short:        "shutdown running Cassandra cluster",
		Example:      fmt.Sprintf(stopExample, "kubectl k8ssandra"),
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.Complete(c, args); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			if err := o.Run(true); err != nil {
				return err
			}

			return nil
		},
	}

	fl := cmd.Flags()
	fl.BoolVarP(&o.wait, "wait", "w", false, "wait until all pods have terminated")
	o.configFlags.AddFlags(fl)
	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error

	if len(args) < 1 {
		return errNoClusterDefined
	}

	c.dcName = args[0]

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	// TODO Parse the --wait
	// if allNamespaces { }

	client, err := cassdcutil.GetClientInNamespace(c.namespace)
	if err != nil {
		return err
	}

	c.cassManager = cassdcutil.NewManager(client)

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	// Verify target cluster exists
	_, err := c.cassManager.CassandraDatacenter(c.dcName, c.namespace)
	if err != nil {
		// NotFound is still an error
		return err
	}
	return nil
}

// Run starts an interactive cqlsh shell on target pod
func (c *options) Run(stop bool) error {
	return c.cassManager.ModifyStoppedState(c.dcName, c.namespace, stop, c.wait)
}
