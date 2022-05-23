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

/*
	Commands are: init, add, commit
*/

/*
	Commit:
		* Install cass-operator using internal helm process (same this client otherwise uses)
		* Create CassandraDatacenter
		* Wait for magic to happen
*/

func (c *ClusterMigrator) FinishInstallation(p *pterm.SpinnerPrinter) error {
	p.UpdateText("Creating CassandraDatacenter")

	err := c.createCassandraDatacenter()
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

func (c *ClusterMigrator) countOfMigratedPods() (int, error) {
	// countOfMigratedPods failed: found 'Cluster', expected: ',' or 'end of string'
	podList := &corev1.PodList{}
	datacenterLabels := map[string]string{
		cassdcapi.DatacenterLabel: c.Datacenter,
		cassdcapi.ClusterLabel:    cassdcapi.CleanupForKubernetes(c.Cluster),
	}
	if err := c.Client.List(context.TODO(), podList, client.MatchingLabels(datacenterLabels)); err != nil {
		return 0, err
	}

	return podList.Size(), nil

}

func (c *ClusterMigrator) createCassandraDatacenter() error {
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
	configFilesMapKey := types.NamespacedName{Name: getConfigMapName(c.Datacenter, "cass-config"), Namespace: c.Namespace}
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
			Name:      c.Datacenter,
			Namespace: c.Namespace,
		},
		Spec: cassdcapi.CassandraDatacenterSpec{
			// TODO There is a cass-operator bug, it creates a label with non-valid characters (such as "Test Cluster")
			ClusterName:   c.Cluster,
			ServerType:    c.ServerType,
			ServerVersion: c.ServerVersion,
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

func (c *ClusterMigrator) waitForDatacenter() error {
	mgr := cassdcutil.NewManager(c.Client)
	dc, err := mgr.CassandraDatacenter(c.Datacenter, c.Namespace)
	if err != nil {
		return err
	}

	return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
		return mgr.RefreshStatus(dc, cassdcapi.DatacenterReady, corev1.ConditionTrue)
	})
}
