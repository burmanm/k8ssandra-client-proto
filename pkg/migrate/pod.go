package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/k8ssandra/cass-operator/pkg/images"
	"github.com/pterm/pterm"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	DefaultTerminationGracePeriodSeconds = 120
	ServerConfigContainerName            = "server-config-init"
	CassandraContainerName               = "cassandra"
	PvcName                              = "server-data"
	SystemLoggerContainerName            = "server-system-logger"
)

func NewNodeMigrator(namespace, cassandraHome string) (*NodeMigrator, error) {
	client, err := cassdcutil.GetClientInNamespace(namespace)
	if err != nil {
		return nil, err
	}

	return &NodeMigrator{
		Client:        client,
		Namespace:     namespace,
		CassandraHome: cassandraHome,
	}, nil
}

func (n *NodeMigrator) MigrateNode(p *pterm.SpinnerPrinter) error {
	// TODO Use the ButterTea or something for prettier output
	p.UpdateText("Getting Cassandra node information")

	// Fetch current node information for cluster+datacenter+rack+hostUUID
	// Fetch the clusterConfig for ordinal selection
	err := n.getNodeInfo()
	if err != nil {
		return err
	}
	pterm.Success.Println("Gathered information from local Cassandra node")

	// Drain and shutdown the current node
	p.UpdateText("Draining and shutting down the current node")
	err = n.drainAndShutdownNode()
	if err != nil {
		return err
	}
	pterm.Success.Println("Local Cassandra node drained and shutdown")

	// Parse configuration..

	// Create PVC + PV
	p.UpdateText("Mounting directories to Kubernetes")
	err = n.createVolumeMounts()
	if err != nil {
		return err
	}
	pterm.Success.Println("Mounted local directories to Kubernetes")

	// Create the pod
	p.UpdateText("Creating pod that runs Cassandra in Kubernetes")
	images.ParseImageConfig("/home/michael/projects/git/datastax/cass-operator/config/manager/image_config.yaml")
	err = n.CreatePod()
	if err != nil {
		return err
	}

	pterm.Success.Println("Created Cassandra pod to the Kubernetes")

	// Run startCassandra on the node
	p.UpdateText("Starting Cassandra node on the Kubernetes cluster")
	pterm.Warning.Println("Failed to start Cassandra node")

	return nil
}

func (n *NodeMigrator) getNodetoolPath() string {
	if n.NodetoolPath != "" {
		return n.NodetoolPath
	}
	return fmt.Sprintf("%s/bin", n.CassandraHome)
}

func (n *NodeMigrator) getNodeInfo() error {
	output, err := execNodetool(n.getNodetoolPath(), "info")
	if err != nil {
		return err
	}

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		columns := strings.Split(line, ":")
		if len(columns) > 1 {
			fieldName := strings.Trim(columns[0], " ")
			fieldValue := columns[1][1:]
			switch fieldName {
			case "ID":
				n.HostID = fieldValue
			case "Rack":
				n.Rack = fieldValue
			case "Data Center":
				n.Datacenter = fieldValue
			}
		}
	}

	configMap := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{Name: configMapName(n.Datacenter), Namespace: n.Namespace}
	if err := n.Client.Get(context.TODO(), configMapKey, configMap); err != nil {
		return err
	}

	ordinalNumber, found := configMap.Data[n.HostID]
	if !found {
		return fmt.Errorf("this node was not part of the init process")
	}
	n.Ordinal = ordinalNumber
	n.ServerType = configMap.Data["serverType"]
	n.ServerVersion = configMap.Data["serverVersion"]
	n.Cluster = configMap.Data["cluster"]

	// TODO Verify kubenode
	kubeNode, err := getLocalKubeNode()
	if err != nil {
		return err
	}
	n.KubeNode = kubeNode

	// fmt.Printf("NodeMigrator: %v\n", n)

	return nil
}

func getLocalKubeNode() (string, error) {
	// TODO This isn't real one yet. Parse from output the correct node based on the local IP
	out, err := exec.Command("/usr/bin/kubectl", "get", "nodes", "-o", "wide").Output()
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(out), "\n")
	columns := strings.Split(lines[1], " ")
	return strings.Trim(columns[0], " "), nil
}

func (n *NodeMigrator) drainAndShutdownNode() error {
	_, err := execNodetool(n.getNodetoolPath(), "drain")
	if err != nil {
		return err
	}

	_, err = execNodetool(n.getNodetoolPath(), "stopdaemon")
	return err
}

func (n *NodeMigrator) getPodName() string {
	return fmt.Sprintf("%s-%s-%s-sts-%s", cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter, n.Rack, n.Ordinal)
}

func (n *NodeMigrator) isSeed() bool {
	// TODO Parse the seed list earlier to catch this
	return true
}

func (n *NodeMigrator) CreatePod() error {
	enableServiceLinks := true

	containers, err := n.buildContainers()
	if err != nil {
		return err
	}

	initContainers, err := n.buildInitContainers()
	if err != nil {
		return err
	}

	volumes, err := n.buildVolumes()
	if err != nil {
		return err
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      n.getPodName(),
			Namespace: n.Namespace,
			Labels: map[string]string{
				"cassandra.datastax.com/seed-node":   strconv.FormatBool(n.isSeed()),
				"statefulset.kubernetes.io/pod-name": n.getPodName(),
			},
		},
		Spec: corev1.PodSpec{
			HostNetwork:        true,
			Affinity:           &corev1.Affinity{},
			Containers:         containers,
			DNSPolicy:          corev1.DNSClusterFirst,
			EnableServiceLinks: &enableServiceLinks,
			Hostname:           n.getPodName(),
			InitContainers:     initContainers,
			NodeName:           n.KubeNode,
			// SecurityContext should mimic whatever is running currently the DSE / Cassandra installation
			SecurityContext: &corev1.PodSecurityContext{},
			Tolerations:     []corev1.Toleration{},
			Volumes:         volumes,
		},
	}

	if err := n.Client.Create(context.TODO(), pod); err != nil {
		return err
	}

	return nil
}

func (n *NodeMigrator) buildVolumes() ([]corev1.Volume, error) {
	volumes := []corev1.Volume{}

	// for _, source := range []string{"server-data", "server-config", "server-logs"} {
	for _, source := range []string{"server-data"} {
		volume := corev1.Volume{
			Name: source,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: n.getPVCName(source),
				},
			},
		}
		volumes = append(volumes, volume)
	}

	vServerConfig := corev1.Volume{
		Name: "server-config",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}

	volumes = append(volumes, vServerConfig)

	vServerLogs := corev1.Volume{
		Name: "server-logs",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}

	volumes = append(volumes, vServerLogs)

	// vServerEncryption := corev1.Volume{
	// 	Name: "encryption-cred-storage",
	// 	VolumeSource: corev1.VolumeSource{
	// 		Secret: &corev1.SecretVolumeSource{
	// 			SecretName: fmt.Sprintf("%s-keystore", n.Datacenter),
	// 		},
	// 	},
	// }
	// volumes = append(volumes, vServerEncryption)

	return volumes, nil
}

func selectorFromFieldPath(fieldPath string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		FieldRef: &corev1.ObjectFieldSelector{
			FieldPath: fieldPath,
		},
	}
}

// This ensure that the server-config-builder init container is properly configured.
func (n *NodeMigrator) buildInitContainers() ([]corev1.Container, error) {

	serverCfg := corev1.Container{}
	serverCfg.Name = ServerConfigContainerName

	serverCfg.Image = images.GetConfigBuilderImage()

	serverCfgMount := corev1.VolumeMount{
		Name:      "server-config",
		MountPath: "/config",
	}

	serverCfg.VolumeMounts = []corev1.VolumeMount{serverCfgMount}

	// serverCfg.Resources = *getResourcesOrDefault(&dc.Spec.ConfigBuilderResources, &DefaultsConfigInitContainer)

	// Convert the bool to a string for the env var setting
	useHostIpForBroadcast := "true"

	envDefaults := []corev1.EnvVar{
		{Name: "POD_IP", ValueFrom: selectorFromFieldPath("status.podIP")},
		{Name: "HOST_IP", ValueFrom: selectorFromFieldPath("status.hostIP")},
		{Name: "USE_HOST_IP_FOR_BROADCAST", Value: useHostIpForBroadcast},
		{Name: "RACK_NAME", Value: n.Rack},
		{Name: "PRODUCT_VERSION", Value: n.ServerVersion},
		{Name: "PRODUCT_NAME", Value: n.ServerType},
		// TODO remove this post 1.0
		// {Name: "DSE_VERSION", Value: serverVersion},
	}

	configEnvVar, err := n.getConfigDataEnVars()
	if err != nil {
		return nil, err
	}
	envDefaults = append(envDefaults, configEnvVar...)

	serverCfg.Env = envDefaults

	return []corev1.Container{serverCfg}, nil
}

func (n *NodeMigrator) getConfigDataEnVars() ([]corev1.EnvVar, error) {
	envVars := make([]corev1.EnvVar, 0)

	configs := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{Name: getConfigMapName(n.Datacenter, "cass-config"), Namespace: n.Namespace}
	if err := n.Client.Get(context.TODO(), configMapKey, configs); err != nil {
		return nil, err
	}

	configData, err := json.Marshal(configs.Data)
	if err != nil {
		return nil, err
	}

	/*
		//    config: |-
		//      {
		//        "cassandra-yaml": {
		//          "read_request_timeout_in_ms": 10000
		//        },
		//        "jmv-options": {
		//          "max_heap_size": 1024M
		//        }
		//      }
	*/

	if err != nil {
		return envVars, err
	}
	envVars = append(envVars, corev1.EnvVar{Name: "CONFIG_FILE_DATA", Value: string(configData)})

	return envVars, nil
}

func (n *NodeMigrator) makeImage() (string, error) {
	return images.GetCassandraImage(n.ServerType, n.ServerVersion)
}

// If values are provided in the matching containers in the
// PodTemplateSpec field of the dc, they will override defaults.
func (n *NodeMigrator) buildContainers() ([]corev1.Container, error) {

	// Create new Container structs or get references to existing ones

	cassContainer := &corev1.Container{}
	loggerContainer := &corev1.Container{}

	// Cassandra container

	cassContainer.Name = CassandraContainerName

	serverImage, err := n.makeImage()
	if err != nil {
		// Could be unsupported DSE version
		return nil, err
	}
	cassContainer.Image = serverImage

	// TODO Container resource restrictions

	cassContainer.LivenessProbe = probe(8080, "/api/v0/probes/liveness", 15, 15)
	cassContainer.ReadinessProbe = probe(8080, "/api/v0/probes/readiness", 20, 10)

	cassContainer.Lifecycle = &corev1.Lifecycle{}

	// This is drain..
	// if cassContainer.Lifecycle.PreStop == nil {
	// 	action, err := httphelper.GetMgmtApiWgetPostAction(dc, httphelper.WgetNodeDrainEndpoint, "")
	// 	if err != nil {
	// 		return err
	// 	}
	// 	cassContainer.Lifecycle.PreStop = &corev1.LifecycleHandler{
	// 		Exec: action,
	// 	}
	// }

	// Combine env vars

	envDefaults := []corev1.EnvVar{
		{Name: "DS_LICENSE", Value: "accept"},
		{Name: "DSE_AUTO_CONF_OFF", Value: "all"},
		{Name: "USE_MGMT_API", Value: "true"},
		{Name: "MGMT_API_EXPLICIT_START", Value: "true"},
		// TODO remove this post 1.0
		{Name: "DSE_MGMT_EXPLICIT_START", Value: "true"},
	}

	// Extra DSE workloads
	// if dc.Spec.ServerType == "dse" && dc.Spec.DseWorkloads != nil {
	// 	envDefaults = append(
	// 		envDefaults,
	// 		corev1.EnvVar{Name: "JVM_EXTRA_OPTS", Value: getJvmExtraOpts(dc)})
	// }

	// cassContainer.Env = combineEnvSlices(envDefaults, cassContainer.Env)

	cassContainer.Env = envDefaults

	// Combine ports

	portDefaults, err := GetContainerPorts()
	if err != nil {
		return nil, err
	}

	// TODO What if user has custom ports?
	cassContainer.Ports = portDefaults

	// Combine volumeMounts

	var volumeMounts []corev1.VolumeMount
	serverCfgMount := corev1.VolumeMount{
		Name:      "server-config",
		MountPath: "/config",
	}
	volumeMounts = append(volumeMounts, serverCfgMount)

	cassServerLogsMount := corev1.VolumeMount{
		Name:      "server-logs",
		MountPath: "/var/log/cassandra",
	}

	volumeMounts = append(volumeMounts,
		[]corev1.VolumeMount{
			cassServerLogsMount,
			{
				Name:      PvcName,
				MountPath: "/var/lib/cassandra",
			},
			// {
			// 	Name:      "encryption-cred-storage",
			// 	MountPath: "/etc/encryption/",
			// },
		}...)

	// volumeMounts = append(volumeMounts, cassContainer.VolumeMounts)
	// cassContainer.VolumeMounts = combineVolumeMountSlices(volumeMounts, generateStorageConfigVolumesMount(dc))
	cassContainer.VolumeMounts = volumeMounts

	// Server Logger Container

	loggerContainer.Name = SystemLoggerContainerName
	loggerContainer.Image = images.GetSystemLoggerImage()

	// volumeMounts = append([]corev1.VolumeMount{cassServerLogsMount}, loggerContainer.VolumeMounts...)

	loggerContainer.VolumeMounts = []corev1.VolumeMount{cassServerLogsMount}

	// loggerContainer.Resources = *getResourcesOrDefault(&dc.Spec.SystemLoggerResources, &DefaultsLoggerContainer)

	return []corev1.Container{*cassContainer, *loggerContainer}, nil
}

func probe(port int, path string, initDelay int, period int) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Port: intstr.FromInt(port),
				Path: path,
			},
		},
		InitialDelaySeconds: int32(initDelay),
		PeriodSeconds:       int32(period),
	}
}

func namedPort(name string, port int) corev1.ContainerPort {
	return corev1.ContainerPort{Name: name, ContainerPort: int32(port)}
}

// GetContainerPorts will return the container ports for the pods in a statefulset based on the provided config
func GetContainerPorts() ([]corev1.ContainerPort, error) {

	nativePort := cassdcapi.DefaultNativePort
	internodePort := cassdcapi.DefaultInternodePort

	// Note: Port Names cannot be more than 15 characters

	ports := []corev1.ContainerPort{
		namedPort("native", nativePort),
		namedPort("tls-native", 9142),
		namedPort("internode", internodePort),
		namedPort("tls-internode", 7001),
		namedPort("jmx", 7199),
		namedPort("mgmt-api-http", 8080),
		namedPort("prometheus", 9103),
		namedPort("thrift", 9160),
	}

	// if dc.Spec.ServerType == "dse" {
	// 	ports = append(
	// 		ports,
	// 		namedPort("internode-msg", 8609),
	// 	)
	// }

	return ports, nil
}
