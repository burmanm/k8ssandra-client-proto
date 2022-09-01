package install

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/burmanm/k8ssandra-client/pkg/helmutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	uninstallExample = `
	# remove management tools from the cluster
	%[1]s uninstall [<args>]

	`
	// errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type uninstallOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace string
}

func newUninstallOptions(streams genericclioptions.IOStreams) *uninstallOptions {
	return &uninstallOptions{
		configFlags: genericclioptions.NewConfigFlags(false),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewUninstallCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newUninstallOptions(streams)

	cmd := &cobra.Command{
		Use:     "uninstall [flags]",
		Short:   "removes management tools from the cluster",
		Example: fmt.Sprintf(installExample, "mcctl"),
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
	o.configFlags.AddFlags(fl)
	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *uninstallOptions) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *uninstallOptions) Validate() error {
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *uninstallOptions) Run() error {
	spinnerLiveText, _ := pterm.DefaultSpinner.Start("Removing management tools...")

	spinnerLiveText.UpdateText("Removing k8ssandra-operator")

	if err := c.uninstallOperator(c.namespace, "mc"); err != nil {
		pterm.Error.Println("Failed to uninstall k8ssandra-operator")
		return err
	}

	pterm.Success.Println("k8ssandra-operator has been uninstalled")

	if err := c.uninstallOperator("cert-manager", "certs"); err != nil {
		pterm.Error.Println("Failed to uninstall cert-manager")
		return err
	}

	pterm.Success.Println("cert-manager has been uninstalled")

	pterm.Info.Println("Management tools have been uninstalled")

	return nil
}

func (c *uninstallOptions) uninstallOperator(namespace, releaseName string) error {
	actionConfig := new(action.Configuration)

	helmDriver := os.Getenv("HELM_DRIVER")
	if err := actionConfig.Init(c.configFlags, namespace, helmDriver, func(format string, v ...interface{}) {}); err != nil {
		log.Fatal(err)
	}

	if _, err := helmutil.Uninstall(actionConfig, releaseName); err != nil {
		pterm.Error.Println("Failed to uninstall k8ssandra-operator")
		if !strings.HasSuffix(err.Error(), "release: not found") {
			return err
		}
	}

	return nil
}
