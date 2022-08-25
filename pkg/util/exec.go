package util

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/cmd/exec"
	"k8s.io/kubectl/pkg/scheme"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

// SetKubernetesDefaults hack is copied from kubectl/exec (unexported)
func SetKubernetesDefaults(config *rest.Config) error {
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

// GetExecOptions returns a filled ExecOptions with correct parameters for Cassandra tools execution
func GetExecOptions(streams genericclioptions.IOStreams, configFlags *genericclioptions.ConfigFlags) (*exec.ExecOptions, error) {
	var err error
	execOptions := &exec.ExecOptions{
		StreamOptions: exec.StreamOptions{
			IOStreams: streams,
		},

		Executor: &exec.DefaultRemoteExecutor{},
	}

	execOptions.Namespace, execOptions.EnforceNamespace, err = configFlags.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return nil, err
	}

	execOptions.ContainerName = "cassandra"

	// execOptions.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	// if err != nil {
	// 	return err
	// }

	execOptions.Config, err = configFlags.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	// Hack from kubectl's exec. Probably a bug in the Kubernetes' client implementation
	SetKubernetesDefaults(execOptions.Config)

	clientset, err := kubernetes.NewForConfig(execOptions.Config)
	if err != nil {
		return nil, err
	}
	execOptions.PodClient = clientset.CoreV1()

	return execOptions, nil
}
