package install

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
	"helm.sh/helm/v3/pkg/cli/values"
	"helm.sh/helm/v3/pkg/getter"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	installExample = `
	# install management tools to the cluster
	%[1]s install [<args>]

	`
	// errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type installOptions struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace string

	// Helm related
	cfg *action.Configuration
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
		Short:   "install management tools to the cluster",
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
func (c *installOptions) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	actionConfig := new(action.Configuration)

	helmDriver := os.Getenv("HELM_DRIVER")
	if err := actionConfig.Init(c.configFlags, c.namespace, helmDriver, func(format string, v ...interface{}) {}); err != nil {
		log.Fatal(err)
	}

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

	restConfig, err := c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	kubeClient, err := cassdcutil.GetClientInNamespace(restConfig, c.namespace)
	if err != nil {
		pterm.Error.Printf("Failed to connect to Kubernetes node: %v", err)
		return err
	}

	pterm.Success.Println("Connected to Kubernetes node")

	spinnerLiveText.UpdateText("Installing cert-manager to the Kubernetes cluster")

	err = cassdcutil.CreateNamespaceIfNotExists(kubeClient, "cert-manager")
	if err != nil {
		return err
	}

	err = c.installCertManager(kubeClient, spinnerLiveText)
	if err != nil {
		return err
	}

	spinnerLiveText.UpdateText("Installing k8ssandra-operator to the Kubernetes cluster")

	err = cassdcutil.CreateNamespaceIfNotExists(kubeClient, c.namespace)
	if err != nil {
		return err
	}

	err = c.installK8ssandraOperator(kubeClient, spinnerLiveText)
	if err != nil {
		return err
	}

	pterm.Info.Println("Management tools have been successfully installed")

	return nil
}

func (c *installOptions) installK8ssandraOperator(kubeClient client.Client, spinnerLiveText *pterm.SpinnerPrinter) error {
	if err := c.installOperator(kubeClient, c.namespace, spinnerLiveText, helmutil.RepoName, helmutil.RepoURL, "k8ssandra-operator", "mc", nil); err != nil {
		pterm.Error.Printf("Failed to install k8ssandra-operator: %v", err)
		return err
	}
	pterm.Success.Println("k8ssandra-operator has been installed")

	spinnerLiveText.UpdateText("Waiting for k8ssandra-operator to start...")

	err := wait.PollImmediate(5*time.Second, 10*time.Minute, func() (bool, error) {
		// depl := &corev1.Deployment{}
		depl := &appsv1.Deployment{}
		deplKey := types.NamespacedName{Name: fmt.Sprintf("%s-k8ssandra-operator", "mc"), Namespace: c.namespace}
		if err := kubeClient.Get(context.TODO(), deplKey, depl); err != nil {
			return false, err
		}
		return depl.Status.ReadyReplicas > 0, nil
	})
	if err != nil {
		return err
	}

	pterm.Success.Println("k8ssandra-operator has started")

	return nil
}

func (c *installOptions) installCertManager(kubeClient client.Client, spinnerLiveText *pterm.SpinnerPrinter) error {
	// kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.9.1/cert-manager.crds.yaml
	// cert-manager doesn't have all the CRDs correctly in their package. For whatever reason
	valueOpts := &values.Options{
		StringValues: []string{"installCRDs=true"},
	}
	if err := c.installOperator(kubeClient, "cert-manager", spinnerLiveText, "jetstack", "https://charts.jetstack.io", "cert-manager", "certs", valueOpts); err != nil {
		return err
	}
	pterm.Success.Println("cert-manager has been installed")

	return nil
}

func (c *installOptions) installOperator(kubeClient client.Client, namespace string, spinnerLiveText *pterm.SpinnerPrinter, repoName, repoURL, chartName, relName string, valueOpts *values.Options) error {
	p := getter.Providers{getter.Provider{
		Schemes: []string{"http", "https"},
		New:     getter.NewHTTPGetter,
	}}
	if valueOpts == nil {
		valueOpts = &values.Options{}
	}
	operatorValues, err := valueOpts.MergeValues(p)
	if err != nil {
		return err
	}

	downloadPath, err := helmutil.DownloadChartRelease(repoName, repoURL, chartName, "")
	if err != nil {
		pterm.Error.Printf("Failed to download %s: %v", chartName, err)
		return err
	}

	pterm.Success.Printf("Downloaded %s chart", chartName)

	_, err = helmutil.Install(c.cfg, relName, downloadPath, namespace, operatorValues)
	if err != nil {
		pterm.Error.Printf("Failed to install %s: %v", chartName, err)
		return err
	}

	pterm.Success.Printf("Installed %s chart", chartName)
	return nil
}
