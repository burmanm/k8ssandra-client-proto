package nodetool

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/cmd/exec"
)

var (
	cqlshExample = `
	# launch a interactive cqlsh shell on node
	%[1]s nodetool <pod> <command> [<args>]
`
	errNoPodDefined     = fmt.Errorf("no target pod defined, could not execute cqlsh")
	errNoCommandDefined = fmt.Errorf("nodetool requires command to execute")
)

type options struct {
	configFlags *genericclioptions.ConfigFlags
	genericclioptions.IOStreams
	namespace   string
	podName     string
	nodeCommand string
	args        []string
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

	// func (p *ExecOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, argsIn []string, argsLenAtDash int) error {

	return cmd
}

// Complete parses the arguments and necessary flags to options
func (c *options) Complete(cmd *cobra.Command, args []string) error {
	var err error

	c.namespace, err = cmd.Flags().GetString("namespace")
	if err != nil {
		return err
	}

	if len(args) > 0 {
		// We just ignore if there's more than 1 pod parameter
		c.podName = args[0]
	}

	if len(args) > 1 {
		c.nodeCommand = args[1]
	}

	if len(args) > 2 {
		c.args = args[2:]
	}

	execOptions := &exec.ExecOptions{
		StreamOptions: exec.StreamOptions{
			IOStreams: c.IOStreams,
		},

		Executor: &exec.DefaultRemoteExecutor{},
	}

	execOptions.PodName = c.podName

	execOptions.Command = []string{"nodetool", "--username X", "--password Y", "status"}
	execOptions.Namespace, execOptions.EnforceNamespace, err = c.configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	execOptions.ContainerName = "cassandra"

	// execOptions.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	// if err != nil {
	// 	return err
	// }

	execOptions.Config, err = c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(execOptions.Config)
	if err != nil {
		return err
	}
	execOptions.PodClient = clientset.CoreV1()

	c.execOptions = execOptions

	// kubectl exec mypod -c ruby-container -i -t -- bash -il

	return nil
}

/*
func (p *ExecOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, argsIn []string, argsLenAtDash int) error {
	if len(argsIn) > 0 && argsLenAtDash != 0 {
		p.ResourceName = argsIn[0]
	}
	if argsLenAtDash > -1 {
		p.Command = argsIn[argsLenAtDash:]
	} else if len(argsIn) > 1 {
		fmt.Fprint(p.ErrOut, "kubectl exec [POD] [COMMAND] is DEPRECATED and will be removed in a future version. Use kubectl exec [POD] -- [COMMAND] instead.\n")
		p.Command = argsIn[1:]
	} else if len(argsIn) > 0 && len(p.FilenameOptions.Filenames) != 0 {
		fmt.Fprint(p.ErrOut, "kubectl exec [POD] [COMMAND] is DEPRECATED and will be removed in a future version. Use kubectl exec [POD] -- [COMMAND] instead.\n")
		p.Command = argsIn[0:]
		p.ResourceName = ""
	}

	var err error
	p.Namespace, p.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	p.ExecutablePodFn = polymorphichelpers.AttachablePodForObjectFn

	p.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	if err != nil {
		return cmdutil.UsageErrorf(cmd, err.Error())
	}

	p.Builder = f.NewBuilder
	p.restClientGetter = f

	cmdParent := cmd.Parent()
	if cmdParent != nil {
		p.ParentCommandName = cmdParent.CommandPath()
	}
	if len(p.ParentCommandName) > 0 && cmdutil.IsSiblingCommandExists(cmd, "describe") {
		p.EnableSuggestedCmdUsage = true
	}

	p.Config, err = f.ToRESTConfig()
	if err != nil {
		return err
	}

	clientset, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}
	p.PodClient = clientset.CoreV1()

	return nil
}
*/

// Validate ensures that all required arguments and flag values are provided
func (c *options) Validate() error {
	if c.podName == "" {
		return errNoPodDefined
	}

	if c.nodeCommand == "" {
		return errNoCommandDefined
	}

	// We could validate here if a nodetool command requires flags, but lets let nodetool throw that error

	return nil
}

// Run triggers the nodetool command on target pod
func (c *options) Run() error {
	// TODO Could be a util .. I guess I'll use this often - move also a lot of the logic to the pkg
	config, err := c.configFlags.ToRESTConfig()
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	secret, err := clientset.CoreV1().Secrets(c.namespace).Get(context.TODO(), "demo-reaper-secret-k8ssandra", metav1.GetOptions{})
	if err != nil {
		return err
	}

	user := secret.Data["username"]
	pass := secret.Data["password"]

	fmt.Printf("Found: %v %v\n", string(user), string(pass))

	// TODO Set container to "cassandra", (-c cassandra)
	// kubectl exec pod -c cassandra ..
	// -- command part should be the after nodetool part

	return c.execOptions.Run()
}
