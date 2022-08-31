package nodetool

import (
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/burmanm/k8ssandra-client/pkg/util"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/exec"
)

var (
	cqlshExample = `
	# launch a interactive cqlsh shell on node
	%[1]s nodetool <pod> <command> [<args>]
`
	errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	execOptions *exec.ExecOptions
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

	if len(args) < 2 {
		return errNotEnoughParameters
	}

	execOptions, err := util.GetExecOptions(c.IOStreams, c.configFlags)
	if err != nil {
		return err
	}
	c.execOptions = execOptions
	execOptions.PodName = args[0]

	restConfig, err := c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	kubeClient, err := cassdcutil.GetClientInNamespace(restConfig, execOptions.Namespace)
	if err != nil {
		return err
	}

	cassSecret, err := util.GetCassandraSuperuserSecrets(kubeClient, execOptions.PodName, execOptions.Namespace)
	if err != nil {
		return err
	}
	execOptions.Command = []string{"nodetool", "--username", cassSecret.Username, "--password", cassSecret.Password, args[1]}
	if len(args) > 2 {
		execOptions.Command = append(execOptions.Command, args[2:]...)
	}

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	// We could validate here if a nodetool command requires flags, but lets let nodetool throw that error

	return nil
}

// Run triggers the nodetool command on target pod
func (c *options) Run() error {
	return c.execOptions.Run()
}
