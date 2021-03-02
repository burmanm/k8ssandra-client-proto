package crds

import (
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/upgrade"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/exec"
)

var (
	upgraderExample = `
	# update CRDs in the namespace to targetVersion
	%[1]s upgradecrds <targetVersion> [<args>]

	`
	errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	execOptions   *exec.ExecOptions
	targetVersion string
	namespace     string
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
		Use:          "upgradecrds <targetVersion> [flags]",
		Short:        "upgrade k8ssandra CRDs to target release version",
		Example:      fmt.Sprintf(upgraderExample, "kubectl k8ssandra"),
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
	if len(args) < 0 {
		return errNotEnoughParameters
	}

	c.targetVersion = args[0]
	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	return err
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	// TODO Validate that the targetVersion is valid
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *options) Run() error {
	upgrader, err := upgrade.New(c.namespace)
	if err != nil {
		return err
	}

	return upgrader.Upgrade(c.targetVersion)
}
