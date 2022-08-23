package migrate

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/burmanm/k8ssandra-client/pkg/helmutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/cli/values"
	"helm.sh/helm/v3/pkg/getter"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

var (
	installExample = `
	# install k8ssandra-operator management tools to the cluster
	%[1]s install [<args>]

	`
	// errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

// const (
// 	releaseName = "migrate"
// )

type installOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace string

	// Helm related
	cfg      *action.Configuration
	settings *cli.EnvSettings
}

func newInstallOptions(streams genericclioptions.IOStreams) *installOptions {
	return &installOptions{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewInstallCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newInstallOptions(streams)

	cmd := &cobra.Command{
		Use:     "install [flags]",
		Short:   "install k8ssandra-operator management tools to the cluster",
		Example: fmt.Sprintf(importExample, "kubectl k8ssandra"),
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
func (c *installOptions) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	// Create new namespace for this usage
	// if c.namespace == "default" || c.namespace == "" {
	// 	c.namespace = releaseName
	// }

	actionConfig := new(action.Configuration)
	settings := cli.New()
	settings.SetNamespace(c.namespace)

	helmDriver := os.Getenv("HELM_DRIVER")
	if err := actionConfig.Init(settings.RESTClientGetter(), c.namespace, helmDriver, func(format string, v ...interface{}) {}); err != nil {
		log.Fatal(err)
	}

	c.settings = settings
	c.cfg = actionConfig

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *installOptions) Validate() error {
	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *installOptions) Run() error {
	spinnerLiveText, _ := pterm.DefaultSpinner.Start("Installing management tools...")

	spinnerLiveText.UpdateText("Creating Kubernetes client to namespace " + c.namespace)

	client, err := cassdcutil.GetClientInNamespace(c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	pterm.Success.Println("Connected to Kubernetes node")

	err = cassdcutil.CreateNamespaceIfNotExists(client, c.namespace)
	if err != nil {
		return err
	}

	spinnerLiveText.UpdateText("Installing k8ssandra-operator to the Kubernetes cluster")

	// TODO Migrate this to ClusterMigrator..

	// cassOperatorValues := map[string]interface{}{}
	p := getter.All(c.settings)
	valueOpts := &values.Options{}
	k8ssandraOperatorValues, err := valueOpts.MergeValues(p)
	if err != nil {
		return err
	}

	downloadPath, err := helmutil.DownloadChartRelease("k8ssandra-operator", "")
	if err != nil {
		pterm.Error.Printf("Failed to download cass-operator: %v", err)
		return err
	}

	pterm.Success.Println("Downloaded k8ssandra-operator chart")

	_, err = helmutil.Install(c.cfg, releaseName, downloadPath, c.namespace, k8ssandraOperatorValues)
	if err != nil {
		pterm.Error.Printf("Failed to install k8ssandra-operator: %v", err)
		return err
	}

	pterm.Success.Println("Installed k8ssandra-operator chart")

	spinnerLiveText.UpdateText("Waiting for k8ssandra-operator to start...")

	err = wait.PollImmediate(5*time.Second, 10*time.Minute, func() (bool, error) {
		// depl := &corev1.Deployment{}
		depl := &appsv1.Deployment{}
		deplKey := types.NamespacedName{Name: fmt.Sprintf("%s-k8ssandra-operator", releaseName), Namespace: c.namespace}
		if err := client.Get(context.TODO(), deplKey, depl); err != nil {
			return false, err
		}
		return depl.Status.ReadyReplicas > 0, nil
	})
	if err != nil {
		return err
	}

	pterm.Success.Println("k8ssandra-operator has started")
	pterm.Info.Println("Management tools have been successfully installed")

	return nil
}
