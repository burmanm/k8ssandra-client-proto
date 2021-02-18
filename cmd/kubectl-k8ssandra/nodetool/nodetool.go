package nodetool

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/cmd/exec"
	"k8s.io/kubectl/pkg/scheme"
)

var (
	cqlshExample = `
	# launch a interactive cqlsh shell on node
	%[1]s nodetool <pod> <command> [<args>]
`
	errNotEnoughParameters = fmt.Errorf("not enough parameters to run nodetool")
)

type options struct {
	namespace   string
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	podName     string
	nodeCommand string
	args        []string
	execOptions *exec.ExecOptions
	restConfig  *rest.Config
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

// setKubernetesDefaults hack is copied from kubectl/exec (unexported)
func setKubernetesDefaults(config *rest.Config) error {
	// TODO remove this hack.  This is allowing the GetOptions to be serialized.
	config.GroupVersion = &schema.GroupVersion{Group: "", Version: "v1"}

	if config.APIPath == "" {
		config.APIPath = "/api"
	}
	if config.NegotiatedSerializer == nil {
		// This codec factory ensures the resources are not converted. Therefore, resources
		// will not be round-tripped through internal versions. Defaulting does not happen
		// on the client.
		config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	}
	return rest.SetKubernetesDefaults(config)
}

// Complete parses the arguments and necessary flags to options
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error

	if len(args) < 2 {
		return errNotEnoughParameters
	}

	c.nodeCommand = args[1]
	if len(args) > 2 {
		c.args = args[2:]
	}

	execOptions := &exec.ExecOptions{
		StreamOptions: exec.StreamOptions{
			IOStreams: c.IOStreams,
		},

		Executor: &exec.DefaultRemoteExecutor{},
	}

	execOptions.PodName = args[0]

	execOptions.Namespace, execOptions.EnforceNamespace, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}
	c.namespace = execOptions.Namespace

	execOptions.ContainerName = "cassandra"

	// execOptions.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	// if err != nil {
	// 	return err
	// }

	c.restConfig, err = c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	// Hack from kubectl's exec. Probably a bug in the Kubernetes' client implementation
	setKubernetesDefaults(c.restConfig)
	execOptions.Config = c.restConfig

	clientset, err := kubernetes.NewForConfig(execOptions.Config)
	if err != nil {
		return err
	}
	execOptions.PodClient = clientset.CoreV1()

	c.execOptions = execOptions

	// Create the correct command line here

	user, pass, _ := c.getUserNamePassword()
	execOptions.Command = []string{"nodetool", "--username", user, "--password", pass, c.nodeCommand}
	if len(c.args) > 0 {
		execOptions.Command = append(execOptions.Command, c.args...)
	}

	return nil
}

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	// We could validate here if a nodetool command requires flags, but lets let nodetool throw that error

	return nil
}

func (c *options) getUserNamePassword() (string, string, error) {
	// TODO Could be a util .. I guess I'll use this often - move also a lot of the logic to the pkg
	clientset, err := kubernetes.NewForConfig(c.restConfig)
	if err != nil {
		return "", "", err
	}

	// TODO Hack for now, this requires a patch in k8ssandra
	secret, err := clientset.CoreV1().Secrets(c.namespace).Get(context.TODO(), "demo-reaper-secret-k8ssandra", metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	user := secret.Data["username"]
	pass := secret.Data["password"]

	return string(user), string(pass), nil
}

// Run triggers the nodetool command on target pod
func (c *options) Run() error {

	// TODO Set container to "cassandra", (-c cassandra)
	// kubectl exec pod -c cassandra ..
	// -- command part should be the after nodetool part

	err := c.execOptions.Run()
	if err != nil {
		return err
	}

	return nil
}
