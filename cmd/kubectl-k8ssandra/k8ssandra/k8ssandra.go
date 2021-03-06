package k8ssandra

import (
	"github.com/burmanm/k8ssandra-client/cmd/kubectl-k8ssandra/cleaner"
	"github.com/burmanm/k8ssandra-client/cmd/kubectl-k8ssandra/cqlsh"
	"github.com/burmanm/k8ssandra-client/cmd/kubectl-k8ssandra/crds"
	"github.com/burmanm/k8ssandra-client/cmd/kubectl-k8ssandra/edit"
	"github.com/burmanm/k8ssandra-client/cmd/kubectl-k8ssandra/nodetool"

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
		Use: "k8ssandra [subcommand] [flags]",
	}

	// Add subcommands
	cmd.AddCommand(nodetool.NewCmd(streams))
	cmd.AddCommand(cqlsh.NewCmd(streams))
	cmd.AddCommand(cleaner.NewCmd(streams))
	cmd.AddCommand(crds.NewCmd(streams))
	cmd.AddCommand(edit.NewCmd(streams))

	// cmd.Flags().BoolVar(&o.listNamespaces, "list", o.listNamespaces, "if true, print the list of all namespaces in the current KUBECONFIG")
	o.configFlags.AddFlags(cmd.Flags())

	return cmd
}
