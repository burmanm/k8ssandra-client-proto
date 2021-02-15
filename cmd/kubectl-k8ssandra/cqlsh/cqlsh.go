package cqlsh

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	cqlshExample = `
	# launch a interactive cqlsh shell on pod
	%[1]s cqlsh <pod>
`
	errNoPodDefined = fmt.Errorf("no target pod defined, could not execute cqlsh")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace string
	podName   string
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
		Use:          "cqlsh [pod] [flags]",
		Short:        "cqlsh launched on pod",
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

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	if c.podName == "" {
		return errNoPodDefined
	}

	return nil
}

// Run starts an interactive cqlsh shell on target pod
func (c *options) Run() error {
	return nil
}
