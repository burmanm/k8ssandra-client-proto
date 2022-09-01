package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

	FSGroupId int

	p *pterm.SpinnerPrinter
}

const (
	ServerData = "server-data"
	// Golang returns rights as bits, so we need to create our own mask..
	// 3 bits per mode, user, group, world order (big-endian)
	// So we want last 6 bits to be: 110 000
	GroupReadAndWriteRights = uint32((1 << 4) | (1 << 5))
)

/*
	Parse all data directories and verify if they have a common root we could
	mount instead of adding all of them as "additional volumes"

	Parse all the _directory and _directories matching keys.
	=> _directories is []string,
		=> data_file_directories
	=> _directory = string
*/

func parseDataPaths(cassandraYaml map[string]interface{}) ([]string, map[string]string, error) {
	additionalDirectories := make(map[string]string)
	dataDirectories := make([]string, 0)

	// TODO We will need additional mounts at least for:
	//		dse.yaml => system_key_directory (encryption keys - how do we handle this?)
	//		dse.yaml => kerberos-file paths (or how does cass-config-builder handle it?)

	for key, val := range cassandraYaml {
		if strings.HasSuffix(key, "_directory") {
			additionalDirectories[key] = val.(string)
		} else if strings.HasSuffix(key, "_directories") {
			dirs := val.([]interface{})
			for _, dataDir := range dirs {
				dataDirectories = append(dataDirectories, dataDir.(string))
			}
		}
	}

	return dataDirectories, additionalDirectories, nil
}

func (n *NodeMigrator) ValidateMountTargets() (int, error) {
	dataDirs, additionalDirs, err := parseDataPaths(n.configs.CassYaml())
	if err != nil {
		return -1, err
	}

	if len(dataDirs) < 1 {
		return -1, fmt.Errorf("no data_file_directories found")
	}

	targetGid := uint32(0)
	for _, dir := range dataDirs {
		gid, err := GetFsGroup(dir)
		if err != nil {
			return -1, err
		}
		if targetGid == 0 {
			targetGid = gid
		}
		if gid != targetGid {
			return -1, fmt.Errorf("found multiple group ids in target directories")
		}
	}

	for _, dir := range additionalDirs {
		gid, err := GetFsGroup(dir)
		if err != nil {
			return -1, err
		}
		if gid != targetGid {
			return -1, fmt.Errorf("found multiple group ids in target directories")
		}

	}

	return int(targetGid), nil
}

func GetFsGroup(path string) (uint32, error) {
	currentGid := uint32(0)
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Couldn't access the file for some reason
			return err
		}

		// Linux only (probably)
		statT := syscall.Stat_t{}
		err = syscall.Stat(path, &statT)
		if err != nil {
			return err
		}
		if currentGid == 0 {
			currentGid = statT.Gid
		}
		if currentGid != statT.Gid {
			return fmt.Errorf("found multiple groups in %s", path)
		}

		return nil
	})

	return currentGid, err
}

func (n *NodeMigrator) FixGroupRights() error {
	dataDirs, additionalDirs, err := parseDataPaths(n.configs.CassYaml())
	if err != nil {
		return err
	}

	for _, val := range dataDirs {
		err = FixDirectoryRights(val)
		if err != nil {
			return err
		}
	}

	for _, val := range additionalDirs {
		err = FixDirectoryRights(val)
		if err != nil {
			return err
		}
	}

	return nil
}

func FixDirectoryRights(path string) error {
	return filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Couldn't access the file for some reason
			return err
		}

		fsInfo, err := os.Stat(path)
		if err != nil {
			return err
		}

		mode := fsInfo.Mode()
		if uint32(mode.Perm())&GroupReadAndWriteRights != GroupReadAndWriteRights {
			modifiedRights := uint32(fsInfo.Mode().Perm()) | GroupReadAndWriteRights
			err = os.Chmod(path, fs.FileMode(modifiedRights))
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (n *NodeMigrator) createVolumeMounts() error {
	dataDirs, additionalDirs, err := parseDataPaths(n.configs.CassYaml())
	if err != nil {
		return err
	}

	if len(dataDirs) < 1 {
		return fmt.Errorf("no data_file_directories found")
	}

	err = n.FixGroupRights()
	if err != nil {
		return err
	}

	// Create server-data first:
	for i := 0; i < len(dataDirs); i++ {
		mountName := ServerData
		if i > 0 {
			mountName = fmt.Sprintf("%s-%d", ServerData, i)
		}

		pv := n.createPV(mountName, dataDirs[0])
		if err := n.Client.Create(context.TODO(), pv); err != nil {
			return err
		}

		pvc := n.createPVC(mountName)
		if err := n.Client.Create(context.TODO(), pvc); err != nil {
			return err
		}

	}

	// Now mount additionalDataDirs:
	for mountName, path := range additionalDirs {
		pv := n.createPV(mountName, path)
		if err := n.Client.Create(context.TODO(), pv); err != nil {
			return err
		}

		pvc := n.createPVC(mountName)
		if err := n.Client.Create(context.TODO(), pvc); err != nil {
			return err
		}
	}

	// TODO Instead of wait, check here that all the PVCs are bound before proceeding
	time.Sleep(10 * time.Second)

	return nil
}

func (n *NodeMigrator) createPVC(mountName string) *corev1.PersistentVolumeClaim {
	volumeMode := new(corev1.PersistentVolumeMode)
	*volumeMode = corev1.PersistentVolumeFilesystem
	storageClassName := "local-path"

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      n.getPVCName(mountName),
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
			VolumeName:       n.getPVName(mountName),
		},
	}
}

func (n *NodeMigrator) createPV(mountName, path string) *corev1.PersistentVolume {
	hostPathType := new(corev1.HostPathType)
	*hostPathType = corev1.HostPathDirectory
	volumeMode := new(corev1.PersistentVolumeMode)
	*volumeMode = corev1.PersistentVolumeFilesystem

	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      n.getPVName(mountName),
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
					Path: path,
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
