package migrate

import (
	"fmt"
	"strconv"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/k8ssandra/cass-operator/pkg/images"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	DefaultTerminationGracePeriodSeconds = 120
	ServerConfigContainerName            = "server-config-init"
	CassandraContainerName               = "cassandra"
	PvcName                              = "server-data"
	SystemLoggerContainerName            = "server-system-logger"
)

func (n *NodeMigrator) getPodName() string {
	return fmt.Sprintf("%s-%s-%s-sts-%d", cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter, n.Rack, n.Ordinal)
}

func (n *NodeMigrator) isSeed() bool {
	// TODO Parse the seed list earlier to catch this
	return true
}

func (n *NodeMigrator) CreatePod() (*corev1.Pod, error) {
	enableServiceLinks := true

	containers, err := buildContainers()
	if err != nil {
		return nil, err
	}

	initContainers, err := buildInitContainers()
	if err != nil {
		return nil, err
	}

	return &corev1.Pod{
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
			Volumes:         []corev1.Volume{},
		},
	}, nil
}

// This ensure that the server-config-builder init container is properly configured.
func buildInitContainers(rackName string) ([]corev1.Container, error) {

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

	configEnvVar, err := getConfigDataEnVars(dc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get config env vars")
	}

	serverVersion := dc.Spec.ServerVersion

	envDefaults := []corev1.EnvVar{
		{Name: "POD_IP", ValueFrom: selectorFromFieldPath("status.podIP")},
		{Name: "HOST_IP", ValueFrom: selectorFromFieldPath("status.hostIP")},
		{Name: "USE_HOST_IP_FOR_BROADCAST", Value: useHostIpForBroadcast},
		{Name: "RACK_NAME", Value: rackName},
		{Name: "PRODUCT_VERSION", Value: serverVersion},
		{Name: "PRODUCT_NAME", Value: dc.Spec.ServerType},
		// TODO remove this post 1.0
		{Name: "DSE_VERSION", Value: serverVersion},
	}

	for _, envVar := range configEnvVar {
		envDefaults = append(envDefaults, envVar)
	}

	serverCfg.Env = envDefaults

	return []corev1.Container{serverCfg}, nil
}

func getConfigDataEnVars(dc *api.CassandraDatacenter) ([]corev1.EnvVar, error) {
	envVars := make([]corev1.EnvVar, 0)

	if len(dc.Spec.ConfigSecret) > 0 {
		envVars = append(envVars, corev1.EnvVar{
			Name: "CONFIG_FILE_DATA",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: getDatacenterConfigSecretName(dc),
					},
					Key: "config",
				},
			},
		})

		if configHash, ok := dc.Annotations[api.ConfigHashAnnotation]; ok {
			envVars = append(envVars, corev1.EnvVar{
				Name:  "CONFIG_HASH",
				Value: configHash,
			})
			return envVars, nil
		}

		return nil, fmt.Errorf("datacenter %s is missing %s annotation", dc.Name, api.ConfigHashAnnotation)
	}

	configData, err := dc.GetConfigAsJSON(dc.Spec.Config)

	if err != nil {
		return envVars, err
	}
	envVars = append(envVars, corev1.EnvVar{Name: "CONFIG_FILE_DATA", Value: configData})

	return envVars, nil
}

func makeImage() (string, error) {
	// TODO Or just use GetCassandraImage directly?
	return images.GetCassandraImage(dc.Spec.ServerType, dc.Spec.ServerVersion)
}

// If values are provided in the matching containers in the
// PodTemplateSpec field of the dc, they will override defaults.
func buildContainers() ([]corev1.Container, error) {

	// Create new Container structs or get references to existing ones

	cassContainer := &corev1.Container{}
	loggerContainer := &corev1.Container{}

	// Cassandra container

	cassContainer.Name = CassandraContainerName

	serverImage, err := makeImage()
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
			{
				Name:      "encryption-cred-storage",
				MountPath: "/etc/encryption/",
			},
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

	if dc.Spec.ServerType == "dse" {
		ports = append(
			ports,
			namedPort("internode-msg", 8609),
		)
	}

	return ports, nil
}
