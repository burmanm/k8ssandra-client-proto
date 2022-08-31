package users

import (
	"context"
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/burmanm/k8ssandra-client/pkg/migrate"
	"github.com/burmanm/k8ssandra-client/pkg/secrets"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	importAddExample = `
	# Add new users to CassandraDatacenter
	%[1]s add [<args>]

	`
	errNoUserPath = fmt.Errorf("path to secret is required")
	errNoDcDc     = fmt.Errorf("target CassandraDatacenter is required")
)

type addOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace  string
	secretPath string
	datacenter string
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
		Use:     "add [flags]",
		Short:   "Add new users to CassandraDatacenter installation",
		Example: fmt.Sprintf(importAddExample, "kubectl k8ssandra users"),
		// SilenceUsage:  true,
		// SilenceErrors: true,
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
	fl.StringVarP(&o.secretPath, "path", "p", "", "path to users data")
	fl.StringVarP(&o.datacenter, "dc", "d", "", "target datacenter")
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
	if c.secretPath == "" {
		return errNoUserPath
	}
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *addOptions) Run() error {
	users, err := secrets.ReadTargetPath(c.secretPath)
	if err != nil {
		return err
	}

	restConfig, err := c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	kubeClient, err := cassdcutil.GetClientInNamespace(restConfig, c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	// Create ManagementClient
	mgmtClient, err := migrate.NewManagementClient(context.TODO(), kubeClient)
	if err != nil {
		return err
	}

	cassManager := cassdcutil.NewManager(kubeClient)
	dc, err := cassManager.CassandraDatacenter(c.datacenter, c.namespace)
	if err != nil {
		return err
	}

	podList, err := cassManager.CassandraDatacenterPods(dc)
	if err != nil {
		return err
	}

	for user, pass := range users {
		// TODO Some error handling is required if the first pod doesn't work
		// Do this for every user .. and move all of this to pkg
		err = mgmtClient.CallCreateRoleEndpoint(&podList.Items[0], user, pass, true)
		if err != nil {
			return err
		}
	}

	return nil
}
