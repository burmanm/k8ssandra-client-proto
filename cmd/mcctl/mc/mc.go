package mc

import (
	"github.com/burmanm/k8ssandra-client/cmd/mcctl/install"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

type ClientOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
}

// NewClientOptions provides an instance of NamespaceOptions with default values
func NewClientOptions(streams genericclioptions.IOStreams) *ClientOptions {
	return &ClientOptions{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping NamespaceOptions
func NewCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := NewClientOptions(streams)

	cmd := &cobra.Command{
		Use: "mcctl [subcommand] [flags]",
	}

	// Add subcommands
	cmd.AddCommand(install.NewInstallCmd(streams))
	cmd.AddCommand(install.NewListCmd(streams))
	cmd.AddCommand(install.NewUninstallCmd(streams))

	// cmd.Flags().BoolVar(&o.listNamespaces, "list", o.listNamespaces, "if true, print the list of all namespaces in the current KUBECONFIG")
	o.configFlags.AddFlags(cmd.Flags())

	return cmd
}
