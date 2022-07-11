package migrate

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/pterm/pterm"
)

type NodeMigrator struct {
	client.Client
	NodetoolPath  string
	CassandraHome string

	configs *ConfigParser

	// Nodetool describecluster has this information (cluster name)
	// Verify we use the same cleanupForKubernetesAPI like cass-operator would (exposed in the apis/v1beta1)
	Cluster string

	// Fetch this information from the nodetool status
	Datacenter string
	Rack       string

	KubeNode  string
	Ordinal   string
	Namespace string
	HostID    string

	ServerType    string
	ServerVersion string

	p *pterm.SpinnerPrinter
}

func (n *NodeMigrator) parseDataPath(dataDir string) string {
	// TODO Parse from configuration
	dataDirName := dataDir[7:]
	if dataDirName == "config" {
		dataDirName = "conf"
	}

	// TODO Or hardcode for the demo?

	// Should be parsed from data_file_directories [array]
	if dirs, found := n.configs.cassandraYaml["data_file_directories"]; found {
		dirs := dirs.([]interface{})
		return dirs[0].(string)
	}

	return fmt.Sprintf("%s/%s", n.CassandraHome, dataDirName)
}

func (n *NodeMigrator) createVolumeMounts() error {
	// dataDir := n.parseDataDirectory()

	// TODO Multiple data directories? Add additionalVolumes here also (for CassDatacenter)
	// for _, dataDir := range []string{"server-logs", "server-config", "server-data"} {
	for _, dataDir := range []string{"server-data"} {
		pv := n.createPV(dataDir, n.parseDataPath(dataDir))
		if err := n.Client.Create(context.TODO(), pv); err != nil {
			return err
		}

		pvc := n.createPVC(dataDir)
		if err := n.Client.Create(context.TODO(), pvc); err != nil {
			return err
		}
	}

	// TODO Instead of wait, check here that all the PVCs are bound before proceeding
	time.Sleep(10 * time.Second)

	return nil
}

func (n *NodeMigrator) createPVC(dataDirectory string) *corev1.PersistentVolumeClaim {
	volumeMode := new(corev1.PersistentVolumeMode)
	*volumeMode = corev1.PersistentVolumeFilesystem
	storageClassName := "local-path"

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      n.getPVCName(dataDirectory),
			Namespace: n.Namespace,
			Annotations: map[string]string{
				"volume.beta.kubernetes.io/storage-provisioner": "rancher.io/local-path",
				"volume.kubernetes.io/selected-node":            n.KubeNode,
			},
			// TODO Do we need labels to indicate what created this? Would be nice to match it with the CassandraDatacenter
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					// TODO Hardcoded not real value
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
			StorageClassName: &storageClassName,
			VolumeMode:       volumeMode,
			VolumeName:       n.getPVName(dataDirectory),
		},
	}
}

func (n *NodeMigrator) createPV(dataDirectory, dataPath string) *corev1.PersistentVolume {
	hostPathType := new(corev1.HostPathType)
	*hostPathType = corev1.HostPathDirectory
	volumeMode := new(corev1.PersistentVolumeMode)
	*volumeMode = corev1.PersistentVolumeFilesystem

	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      n.getPVName(dataDirectory),
			Namespace: n.Namespace,
			Annotations: map[string]string{
				"pv.kubernetes.io/provisioned-by": "rancher.io/local-path",
			},
			// TODO Do we need labels to indicate what created this? Would be nice to match it with the CassandraDatacenter
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Capacity: corev1.ResourceList{
				// TODO Hardcoded not real value
				corev1.ResourceStorage: resource.MustParse("5Gi"),
			},
			StorageClassName: "local-path",
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Type: hostPathType,
					Path: dataPath,
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			VolumeMode:                    volumeMode,
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: corev1.NodeSelectorOpIn,
									Values: []string{
										n.KubeNode,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (n *NodeMigrator) getPVName(dataDirectory string) string {
	return fmt.Sprintf("pvc-%s", n.getPVCName(dataDirectory))
}

func (n *NodeMigrator) getPVCName(dataDirectory string) string {
	return fmt.Sprintf("%s-%s-%s-%s-sts-%s", dataDirectory, cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter, n.Rack, n.Ordinal)
}
