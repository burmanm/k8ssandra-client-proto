package nodetool

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	cqlshExample = `
	# launch a interactive cqlsh shell on node
	%[1]s nodetool <pod> <command> [<args>]
`
	errNoPodDefined     = fmt.Errorf("no target pod defined, could not execute cqlsh")
	errNoCommandDefined = fmt.Errorf("nodetool requires command to execute")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace   string
	podName     string
	nodeCommand string
	args        []string
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
		Use:          "nodetool [pod] [flags]",
		Short:        "nodetool launched on pod",
		Example:      fmt.Sprintf(cqlshExample, "kubectl k8ssandra"),
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

	o.configFlags.AddFlags(cmd.Flags())

	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, err = cmd.Flags().GetString("namespace")
	if err != nil {
		return err
	}

	if len(args) > 0 {
		// We just ignore if there's more than 1 pod parameter
		c.podName = args[0]
	}

	if len(args) > 1 {
		c.nodeCommand = args[1]
	}

	if len(args) > 2 {
		c.args = args[2:]
	}

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	if c.podName == "" {
		return errNoPodDefined
	}

	if c.nodeCommand == "" {
		return errNoCommandDefined
	}

	// We could validate here if a nodetool command requires flags, but lets let nodetool throw that error

	return nil
}

// Run triggers the nodetool command on target pod
func (c *options) Run() error {
	return nil
}
