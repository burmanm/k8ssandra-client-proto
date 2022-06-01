package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/k8ssandra/cass-operator/pkg/images"
	"github.com/k8ssandra/cass-operator/pkg/serverconfig"
	"github.com/pterm/pterm"
	"gopkg.in/yaml.v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	waitutil "k8s.io/apimachinery/pkg/util/wait"
)

const (
	DefaultTerminationGracePeriodSeconds = 120
	ServerConfigContainerName            = "server-config-init"
	CassandraContainerName               = "cassandra"
	PvcName                              = "server-data"
	SystemLoggerContainerName            = "server-system-logger"
)

func NewNodeMigrator(cli client.Client, namespace, cassandraHome string) *NodeMigrator {
	return &NodeMigrator{
		Client:        cli,
		Namespace:     namespace,
		CassandraHome: cassandraHome,
	}
}

func (n *NodeMigrator) MigrateNode(p *pterm.SpinnerPrinter) error {
	n.p = p
	// TODO Use the ButterTea or something for prettier output
	p.UpdateText("Getting Cassandra node information")

	cfgParser := NewParser(n.CassandraHome)
	if err := cfgParser.ParseConfigs(); err != nil {
		return err
	}

	// Fetch current node information for cluster+datacenter+rack+hostUUID
	// Fetch the clusterConfig for ordinal selection
	if err := n.getNodeInfo(cfgParser.CassYaml()); err != nil {
		return err
	}
	pterm.Success.Println("Gathered information from local Cassandra node")

	// Drain and shutdown the current node
	p.UpdateText("Draining and shutting down the current node")
	if err := n.drainAndShutdownNode(); err != nil {
		return err
	}
	pterm.Success.Println("Local Cassandra node drained and shutdown")

	// Parse configuration..

	// Create PVC + PV
	p.UpdateText("Mounting directories to Kubernetes")
	if err := n.createVolumeMounts(); err != nil {
		return err
	}
	pterm.Success.Println("Mounted local directories to Kubernetes")

	// Create the pod
	p.UpdateText("Creating pod that runs Cassandra in Kubernetes")
	images.ParseImageConfig("/home/michael/projects/git/datastax/cass-operator/config/manager/image_config.yaml")
	if err := n.CreatePod(); err != nil {
		return err
	}

	pterm.Success.Println("Created Cassandra pod to the Kubernetes")

	// Run startCassandra on the node
	p.UpdateText("Starting Cassandra node on the Kubernetes cluster")

	if err := n.StartPod(); err != nil {
		return err
	}

	pterm.Success.Println("Cassandra pod has successfully started")
	// pterm.Warning.Println("Failed to start Cassandra node")

	return nil
}

func (n *NodeMigrator) getNodetoolPath() string {
	if n.NodetoolPath != "" {
		return n.NodetoolPath
	}
	return fmt.Sprintf("%s/bin", n.CassandraHome)
}

func (n *NodeMigrator) getNodeInfo(cassConfig map[string]interface{}) error {
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

	// TODO Unmarshal this one..
	b := configMap.BinaryData["clusterInfo"]

	clusterConfigMap := ClusterConfigMap{}
	err = json.Unmarshal(b, &clusterConfigMap)
	if err != nil {
		return err
	}

	for _, nodeInfo := range clusterConfigMap.NodeInfos {
		if nodeInfo.HostId == n.HostID {
			n.Ordinal = nodeInfo.Ordinal
			break
		}
	}

	if n.Ordinal == "" {
		return fmt.Errorf("this node was not part of the init process")
	}

	n.ServerType = clusterConfigMap.ServerType
	n.ServerVersion = clusterConfigMap.ServerVersion
	n.Cluster = clusterConfigMap.Cluster

	// TODO Verify kubenode
	kubeNode, err := n.getLocalKubeNode(cassConfig)
	if err != nil {
		return err
	}
	n.KubeNode = kubeNode

	return nil
}

func (n *NodeMigrator) getLocalKubeNode(cassConfig map[string]interface{}) (string, error) {
	// TODO Use client to get all the nodes and compare the IP address
	nodes := &corev1.NodeList{}
	if err := n.Client.List(context.TODO(), nodes); err != nil {
		return "", err
	}

	// TODO What about IPv6?

	targetIP := ""

	if addr, found := cassConfig["listen_address"]; found {
		ipAddr := addr.(string)
		if ipAddr != "" && ipAddr != "0.0.0.0" {
			// TODO Should not be loopback either
			targetIP = ipAddr
		}
	}

	if eth, found := cassConfig["listen_interface"]; found {
		ethName := eth.(string)
		ifaces, err := net.Interfaces()
		if err != nil {
			return "", err
		}

		for _, i := range ifaces {
			if i.Name != ethName {
				continue
			}

			addrs, err := i.Addrs()
			if err != nil {
				return "", err
			}
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if ip != nil {
					targetIP = ip.String()
					break
				}
			}
		}
	}

	if targetIP != "" {
		for _, node := range nodes.Items {
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeInternalIP {
					if addr.Address == targetIP {
						return node.Name, nil
					}
					break
				}
			}
		}
	}

	return "", fmt.Errorf("failed to find local Kubernetes node")
}

func (n *NodeMigrator) drainAndShutdownNode() error {
	_, err := execNodetool(n.getNodetoolPath(), "drain")
	if err != nil {
		return err
	}

	_, err = execNodetool(n.getNodetoolPath(), "stopdaemon")
	return err
}

func (n *NodeMigrator) getGenerateName() string {
	return fmt.Sprintf("%s-%s-%s-sts-", cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter, n.Rack)
}

func (n *NodeMigrator) getPodName() string {
	return n.getGenerateName() + n.Ordinal
	// return fmt.Sprintf("%s-%s-%s-sts-%s", cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter, n.Rack, n.Ordinal)
}

func (n *NodeMigrator) getAllPodsServiceName() string {
	return fmt.Sprintf("%s-%s-all-pods-service", cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter)
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

	userId := int64(999)
	userGroup := int64(999)
	// TODO A placeholder in the dev machine
	fsGroup := int64(1001)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:         n.getPodName(),
			Namespace:    n.Namespace,
			GenerateName: n.getGenerateName(),
			Labels: map[string]string{
				"statefulset.kubernetes.io/pod-name": n.getPodName(),
				cassdcapi.SeedNodeLabel:              strconv.FormatBool(n.isSeed()),
				cassdcapi.RackLabel:                  n.Rack,
				cassdcapi.ClusterLabel:               cassdcapi.CleanupForKubernetes(n.Cluster),
				cassdcapi.DatacenterLabel:            n.Datacenter,
			},
		},
		Spec: corev1.PodSpec{
			HostNetwork:        true,
			Affinity:           n.podAffinity(),
			Containers:         containers,
			DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
			EnableServiceLinks: &enableServiceLinks,
			Hostname:           n.getPodName(),
			Subdomain:          n.getAllPodsServiceName(),
			InitContainers:     initContainers,
			NodeName:           n.KubeNode,
			// SecurityContext should mimic whatever is running currently the DSE / Cassandra installation
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:  &userId,
				RunAsGroup: &userGroup,
				FSGroup:    &fsGroup,
			},
			Tolerations: []corev1.Toleration{},
			Volumes:     volumes,
		},
	}

	if err := n.Client.Create(context.TODO(), pod); err != nil {
		return err
	}

	return nil
}

func (n *NodeMigrator) podAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      corev1.LabelHostname,
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{n.KubeNode},
							},
						},
					},
				},
			},
		},
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key:      cassdcapi.ClusterLabel,
								Operator: metav1.LabelSelectorOpExists,
							},
							{
								Key:      cassdcapi.DatacenterLabel,
								Operator: metav1.LabelSelectorOpExists,
							},
							{
								Key:      cassdcapi.RackLabel,
								Operator: metav1.LabelSelectorOpExists,
							},
						},
					},
					TopologyKey: corev1.LabelHostname,
				},
			},
		},
	}
}

func (n *NodeMigrator) StartPod() error {
	// TODO Could we instead of host networking also use nodeReplace to replace all the existing nodes with the data we already have? Thus moving to Kubernetes
	// networking?

	// Create ManagementClient
	mgmtClient, err := NewManagementClient(context.TODO(), n.Client)
	if err != nil {
		return err
	}

	// Get the pod
	podKey := types.NamespacedName{Name: n.getPodName(), Namespace: n.Namespace}
	pod := &corev1.Pod{}
	if err := n.Client.Get(context.TODO(), podKey, pod); err != nil {
		return err
	}

	// Wait until the pod is ready to start
	err = waitutil.PollImmediate(5*time.Second, 10*time.Minute, func() (bool, error) {
		if err := n.Client.Get(context.TODO(), podKey, pod); err != nil {
			return false, err
		}
		return isMgmtApiRunning(pod), nil
	})

	if err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	pterm.Success.Println("Management API has started")

	// n.p.UpdateText("Calling Cassandra start...")
	// Call the Cassandra start
	err = mgmtClient.CallLifecycleStartEndpoint(pod)
	if err != nil {
		return err
	}

	// Wait until the pod has started
	err = waitutil.PollImmediate(5*time.Second, 10*time.Minute, func() (bool, error) {
		if err := n.Client.Get(context.TODO(), podKey, pod); err != nil {
			return false, err
		}
		return isServerReady(pod), nil
	})
	if err != nil {
		return err
	}

	// Update podLabel to indicate pod has started
	if err := n.Client.Get(context.TODO(), podKey, pod); err != nil {
		return err
	}

	pod.Labels[cassdcapi.CassNodeState] = "Started"

	if err := n.Client.Update(context.TODO(), pod); err != nil {
		return err
	}

	return nil
}

// from cass-operator
func isMgmtApiRunning(pod *corev1.Pod) bool {
	podStatus := pod.Status
	statuses := podStatus.ContainerStatuses
	for _, status := range statuses {
		if status.Name != "cassandra" {
			continue
		}
		state := status.State
		runInfo := state.Running
		if runInfo != nil {
			// give management API ten seconds to come up
			tenSecondsAgo := time.Now().Add(time.Second * -10)
			return runInfo.StartedAt.Time.Before(tenSecondsAgo)
		}
	}
	return false
}

func isServerReady(pod *corev1.Pod) bool {
	status := pod.Status
	statuses := status.ContainerStatuses
	for _, status := range statuses {
		if status.Name != "cassandra" {
			continue
		}
		return status.Ready
	}
	return false
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

func (n *NodeMigrator) getModelValues() serverconfig.NodeConfig {
	additionalSeedServiceName := cassdcapi.CleanupForKubernetes(n.Cluster) + "-" + n.Datacenter + "-additional-seed-service"
	seedServiceName := cassdcapi.CleanupForKubernetes(n.Cluster) + "-seed-service"

	seeds := []string{seedServiceName, additionalSeedServiceName}

	native := 0
	nativeSSL := 0
	internode := 0
	internodeSSL := 0
	graphEnabled := 0
	solrEnabled := 0
	sparkEnabled := 0

	modelValues := serverconfig.GetModelValues(
		seeds,
		n.Cluster,
		n.Datacenter,
		graphEnabled,
		solrEnabled,
		sparkEnabled,
		native,
		nativeSSL,
		internode,
		internodeSSL)

	return modelValues

	// var modelBytes []byte

	// modelBytes, err := json.Marshal(modelValues)
	// if err != nil {
	// 	return "", err
	// }
}

func (n *NodeMigrator) getConfigDataEnVars() ([]corev1.EnvVar, error) {
	envVars := make([]corev1.EnvVar, 0)

	configs := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{Name: getConfigMapName(n.Datacenter, "cass-config"), Namespace: n.Namespace}
	if err := n.Client.Get(context.TODO(), configMapKey, configs); err != nil {
		return nil, err
	}

	modelValues := n.getModelValues()
	// configsData := make(map[string]map[string]interface{})

	for k, v := range configs.Data {
		yamlData := make(serverconfig.NodeConfig)
		if err := yaml.Unmarshal([]byte(v), &yamlData); err != nil {
			return nil, err
		}
		modelValues[k] = yamlData
	}

	configData, err := json.Marshal(modelValues)
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

	// This is drain.. perhaps our reconcile once cass-operator is up will add this? We don't have DC spec created yet
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
