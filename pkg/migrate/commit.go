package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/pterm/pterm"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	waitutil "k8s.io/apimachinery/pkg/util/wait"
)

type MigrateFinisher struct {
	client.Client
	namespace        string
	datacenter       string
	clusterConfigMap ClusterConfigMap
}

func NewMigrateFinisher(cli client.Client, namespace, datacenter string) *MigrateFinisher {
	return &MigrateFinisher{
		Client:     cli,
		namespace:  namespace,
		datacenter: datacenter,
	}
}

func (c *MigrateFinisher) fetchConfiguration() error {
	configMap := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{Name: configMapName(c.datacenter), Namespace: c.namespace}
	if err := c.Client.Get(context.TODO(), configMapKey, configMap); err != nil {
		return err
	}

	b := configMap.BinaryData["clusterInfo"]

	clusterConfigMap := ClusterConfigMap{}
	if err := json.Unmarshal(b, &clusterConfigMap); err != nil {
		return err
	}

	c.clusterConfigMap = clusterConfigMap

	return nil
}

func (c *MigrateFinisher) FinishInstallation(p *pterm.SpinnerPrinter) error {

	p.UpdateText("Fetching cluster configuration...")

	err := c.fetchConfiguration()
	if err != nil {
		return err
	}

	pterm.Success.Println("Cluster configuration fetched")

	p.UpdateText("Creating CassandraDatacenter")

	err = c.createCassandraDatacenter()
	if err != nil {
		return err
	}

	pterm.Success.Println("CassandraDatacenter definition created")

	p.UpdateText("Waiting for Datacenter to finish reconciliation...")

	err = c.waitForDatacenter()
	if err != nil {
		return err
	}

	pterm.Success.Println("CassandraDatacenter status is Ready")

	pterm.Info.Println("Cluster is fully managed now, welcome to k8ssandra")

	return nil
}

func (c *MigrateFinisher) countOfMigratedPods() (int, error) {
	// countOfMigratedPods failed: found 'Cluster', expected: ',' or 'end of string'
	podList := &corev1.PodList{}
	datacenterLabels := map[string]string{
		cassdcapi.DatacenterLabel: c.clusterConfigMap.Datacenter,
		cassdcapi.ClusterLabel:    cassdcapi.CleanupForKubernetes(c.clusterConfigMap.Cluster),
	}
	if err := c.Client.List(context.TODO(), podList, client.MatchingLabels(datacenterLabels)); err != nil {
		return 0, err
	}

	return podList.Size(), nil

}

func (c *MigrateFinisher) createCassandraDatacenter() error {
	// Fetch the amount of pods we created to ensure all the pods have been
	// migrated before we continue
	datacenterSize, err := c.countOfMigratedPods()
	if err != nil {
		fmt.Printf("countOfMigratedPods failed: %v\n", err)
		return err
	}
	// datacenterSize = len(c.clusterConfigMap.NodeInfos)

	if datacenterSize != len(c.clusterConfigMap.NodeInfos) {
		datacenterSize = len(c.clusterConfigMap.NodeInfos)
		// return fmt.Errorf("not all nodes were migrated yet, can't continue")
	}

	clusterRacks := make(map[string]bool)
	for _, nodeInfo := range c.clusterConfigMap.NodeInfos {
		clusterRacks[nodeInfo.Rack] = true
	}

	racks := []cassdcapi.Rack{}

	for rackInfo := range clusterRacks {
		racks = append(racks, cassdcapi.Rack{
			Name: rackInfo,
		})
	}
	storageClassName := "local-path"
	userId := int64(999)
	userGroup := int64(999)
	// TODO A placeholder in the dev machine
	fsGroup := int64(1001)

	// TODO Move to another function
	configFilesMap := &corev1.ConfigMap{}
	configFilesMapKey := types.NamespacedName{Name: getConfigMapName(c.clusterConfigMap.Datacenter, "cass-config"), Namespace: c.namespace}
	if err := c.Client.Get(context.TODO(), configFilesMapKey, configFilesMap); err != nil {
		return err
	}

	cassandraYaml := configFilesMap.Data["cassandra-yaml"]
	modelValues := make(map[string]interface{})
	if err := yaml.Unmarshal([]byte(cassandraYaml), modelValues); err != nil {
		return err
	}

	config := map[string]interface{}{
		"cassandra-yaml": modelValues,
	}

	modelBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}

	dc := &cassdcapi.CassandraDatacenter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.clusterConfigMap.Datacenter,
			Namespace: c.namespace,
		},
		Spec: cassdcapi.CassandraDatacenterSpec{
			// TODO There is a cass-operator bug, it creates a label with non-valid characters (such as "Test Cluster")
			ClusterName:   c.clusterConfigMap.Cluster,
			ServerType:    c.clusterConfigMap.ServerType,
			ServerVersion: c.clusterConfigMap.ServerVersion,
			ManagementApiAuth: cassdcapi.ManagementApiAuthConfig{
				Insecure: &cassdcapi.ManagementApiAuthInsecureConfig{},
			},
			Size:  int32(datacenterSize),
			Racks: racks,
			Networking: &cassdcapi.NetworkingConfig{
				HostNetwork: true,
			},
			StorageConfig: cassdcapi.StorageConfig{
				CassandraDataVolumeClaimSpec: &corev1.PersistentVolumeClaimSpec{
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
				},
			},
			PodTemplateSpec: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: CassandraContainerName,
						},
					},
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  &userId,
						RunAsGroup: &userGroup,
						FSGroup:    &fsGroup,
					},
				},
			},
			Config: modelBytes,
		},
	}

	if err := c.Client.Create(context.TODO(), dc); err != nil {
		fmt.Printf("Failed to insert CassDc, CassDc: %v", dc)
		return err
	}
	return nil
}

func (c *MigrateFinisher) waitForDatacenter() error {
	mgr := cassdcutil.NewManager(c.Client)
	dc, err := mgr.CassandraDatacenter(c.clusterConfigMap.Datacenter, c.namespace)
	if err != nil {
		return err
	}

	return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
		return mgr.RefreshStatus(dc, cassdcapi.DatacenterReady, corev1.ConditionTrue)
	})
}
