package migrate

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/burmanm/k8ssandra-client/pkg/helmutil"
	"github.com/burmanm/k8ssandra-client/pkg/migrate"
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
	importExample = `
	# initialize Kubernetes for Cassandra migration
	%[1]s import init [<args>]

	# Use nodetool from outside $PATH
	%[1]s import init --cassandra-home=$CASSANDRA_HOME

	`
	// errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

const (
	releaseName = "migrate"
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace     string
	nodetoolPath  string
	cassandraHome string
	dseConfigDir  string
	cassConfigDir string
	configDir     string

	// Helm related
	cfg      *action.Configuration
	settings *cli.EnvSettings
}

func newOptions(streams genericclioptions.IOStreams) *options {
	return &options{
		configFlags: genericclioptions.NewConfigFlags(true),
		IOStreams:   streams,
	}
}

// NewCmd provides a cobra command wrapping cqlShOptions
func NewInitCmd(streams genericclioptions.IOStreams) *cobra.Command {
	o := newOptions(streams)

	cmd := &cobra.Command{
		Use:     "init [flags]",
		Short:   "initialize importing Cassandra installation to Kubernetes",
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
	fl.StringVarP(&o.nodetoolPath, "nodetool-path", "p", "", "path to nodetool executable directory")
	fl.StringVarP(&o.cassandraHome, "cassandra-home", "c", "", "path to cassandra/DSE installation directory")
	fl.StringVarP(&o.cassConfigDir, "cass-config-dir", "c", "", "override cassandra.yaml configuration directory")
	fl.StringVarP(&o.dseConfigDir, "dse-config-dir", "c", "", "override dse.yaml configuration directory")
	o.configFlags.AddFlags(fl)
	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, _, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

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
func (c *options) Validate() error {
	cassandraHome, nodetoolPath, err := migrate.DetectInstallation(c.cassandraHome, c.nodetoolPath)
	if err != nil {
		return err
	}
	c.cassandraHome = cassandraHome
	c.nodetoolPath = nodetoolPath

	return nil
}

// Run removes the finalizers for a release X in the given namespace
func (c *options) Run() error {
	spinnerLiveText, _ := pterm.DefaultSpinner.Start("Gathering information for node migration...")

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

	err = cassdcutil.CreateNamespaceIfNotExists(kubeClient, c.namespace)
	if err != nil {
		return err
	}

	migrator, err := migrate.NewClusterMigrator(kubeClient, c.namespace, c.configDir)
	if err != nil {
		return err
	}

	// TODO All of this is in the install command already

	spinnerLiveText.UpdateText("Installing cass-operator to the Kubernetes cluster")

	// TODO Migrate this to ClusterMigrator..

	// cassOperatorValues := map[string]interface{}{}
	p := getter.All(c.settings)
	valueOpts := &values.Options{}
	cassOperatorValues, err := valueOpts.MergeValues(p)
	if err != nil {
		return err
	}

	downloadPath, err := helmutil.DownloadChartRelease(helmutil.RepoName, helmutil.RepoURL, "cass-operator", "0.37.0")
	if err != nil {
		pterm.Error.Printf("Failed to download cass-operator: %v", err)
		return err
	}

	pterm.Success.Println("Downloaded cass-operator chart")

	_, err = helmutil.Install(c.cfg, releaseName, downloadPath, c.namespace, cassOperatorValues)
	if err != nil {
		pterm.Error.Printf("Failed to install cass-operator: %v", err)
		return err
	}

	pterm.Success.Println("Installed cass-operator chart")

	spinnerLiveText.UpdateText("Waiting for cass-operator to start...")

	err = wait.PollImmediate(5*time.Second, 10*time.Minute, func() (bool, error) {
		// depl := &corev1.Deployment{}
		depl := &appsv1.Deployment{}
		deplKey := types.NamespacedName{Name: fmt.Sprintf("%s-cass-operator", releaseName), Namespace: c.namespace}
		if err := kubeClient.Get(context.TODO(), deplKey, depl); err != nil {
			return false, err
		}
		return depl.Status.ReadyReplicas > 0, nil
	})
	if err != nil {
		return err
	}

	pterm.Success.Println("cass-operator has started")

	err = migrator.InitCluster(spinnerLiveText)
	if err != nil {
		pterm.Error.Printf("Failed to connect to local Cassandra node to fetch required information: %v", err)
		return err
	}

	// TODO We need authentication support for nodetool operations
	// TODO We need to support node replacement while the process of migration is going on. We could just allow manual
	// 	 	edit of the ConfigMaps in the cluster as a simple way..

	return nil
}
