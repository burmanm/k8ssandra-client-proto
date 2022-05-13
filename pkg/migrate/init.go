package migrate

import (
	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	"github.com/pterm/pterm"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

/*
	Init:
		* nodetool getseeds
			* Create ConfigMap to indicate host/UUID -> isSeed
			* Write the seeds-service with seed IPs
		* nodetool status / describecluster / etc:
			* Create ConfigMap with cluster knowledge:
				* hostUUID -> ordinal
				* serverType
				* serverVersion
				* clusterName
				* datacenterName
				* hostUUID -> rackName
*/

const (
	ClusterConfigMapName = "clusterDetails"
)

type ClusterMigrator struct {
	client.Client
}

func NewClusterMigrator(namespace string) (*ClusterMigrator, error) {
	client, err := cassdcutil.GetClientInNamespace(namespace)
	if err != nil {
		return nil, err
	}

	return &ClusterMigrator{
		Client: client,
	}, nil
}

func (c *ClusterMigrator) InitCluster() error {
	p, err := pterm.DefaultProgressbar.WithTitle("Parsing cluster details").WithShowCount(false).WithShowPercentage(false).Start()
	if err != nil {
		return err
	}

	p.UpdateTitle("Fetching cluster seeds")
	err = c.CreateSeedService()
	if err != nil {
		pterm.Fatal.Println("Failed to get cluster seeds")
		return err
	}
	pterm.Success.Println("Created seeds service")

	p.UpdateTitle("Fetching cluster details")
	err = c.CreateClusterConfigMap()
	if err != nil {
		pterm.Fatal.Println("Failed to get cluster details")
		return err
	}

	pterm.Success.Println("Cassandra cluster details stored to Kubernetes")

	p.Stop()

	pterm.Info.Println("You can now import nodes to the Kubernetes")

	return nil
}

func (c *ClusterMigrator) getSeeds() {
	// TODO nodetool getseeds
}

func (c *ClusterMigrator) CreateSeedService() error {
	return nil
}

func (c *ClusterMigrator) CreateClusterConfigMap() error {
	return nil
}
