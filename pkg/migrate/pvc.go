package migrate

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
)

type NodeMigrator struct {
	client.Client
	NodetoolPath string

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
}

func (n *NodeMigrator) CreatePVC(dataDirectory string) *corev1.PersistentVolumeClaim {
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
			// TODO Do we need labels to indicate what created this?
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

func (n *NodeMigrator) CreatePV(dataDirectory string) *corev1.PersistentVolume {
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
			// TODO Do we need labels to indicate what created this?
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
					Path: dataDirectory,
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
	// TODO Fix this..
	return fmt.Sprintf("%s-%s", dataDirectory, n.Ordinal)
}

func (n *NodeMigrator) getPVCName(dataDirectory string) string {
	return fmt.Sprintf("%s-%s-%s-%s-sts-%s", dataDirectory, cassdcapi.CleanupForKubernetes(n.Cluster), n.Datacenter, n.Rack, n.Ordinal)
}

/*
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  annotations:
    volume.beta.kubernetes.io/storage-provisioner: rancher.io/local-path
    volume.kubernetes.io/selected-node: ${NODE_NAME}
  finalizers:
  - kubernetes.io/pvc-protection
  labels:
    app.kubernetes.io/instance: cassandra-${CLUSTER_NAME}
    app.kubernetes.io/managed-by: cass-operator
    app.kubernetes.io/name: cassandra
    app.kubernetes.io/version: 4.0.1
    cassandra.datastax.com/cluster: ${CLUSTER_NAME}
    cassandra.datastax.com/datacenter: ${DC_NAME}
    cassandra.datastax.com/rack: ${RACK_NAME}
  name: ${PVC_NAME}
  namespace: cass-operator
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: local-path
  volumeMode: Filesystem
  volumeName: ${PV_NAME}

*/

/*
apiVersion: v1
kind: PersistentVolume
metadata:
  annotations:
    pv.kubernetes.io/provisioned-by: rancher.io/local-path
  finalizers:
  - kubernetes.io/pv-protection
  name: ${PV_NAME}
spec:
  accessModes:
  - ReadWriteOnce
  capacity:
    storage: 5Gi
  hostPath:
    path: ${DATA_PATH}
    type: DirectoryOrCreate
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - ${KUBE_NODE}
  persistentVolumeReclaimPolicy: Retain
  storageClassName: local-path
  volumeMode: Filesystem
*/
