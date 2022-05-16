package migrate

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/pterm/pterm"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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
	NodetoolPath string

	// TODO Merge ClusterMigrator and NodeMigrator?

	Cluster    string
	Datacenter string
	Rack       string

	KubeNode  string
	Ordinal   int
	Namespace string
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

	// TODO Replace with BubbleTea

	p.UpdateTitle("Fetching cluster details")
	err = c.CreateClusterConfigMap()
	if err != nil {
		fmt.Printf("Failed to get cluster details: %v\n", err)
		// pterm.Fatal.Println("Failed to get cluster details")
		return err
	}

	pterm.Success.Println("Cassandra cluster details stored to Kubernetes")

	p.UpdateTitle("Fetching cluster seeds")
	err = c.CreateSeedServices()
	if err != nil {
		fmt.Printf("Failed to get cluster seeds: %v\n", err)
		pterm.Fatal.Println("Failed to get cluster seeds")
		return err
	}
	pterm.Success.Println("Created seeds service")

	p.Stop()

	pterm.Info.Println("You can now import nodes to the Kubernetes")

	return nil
}

func (c *ClusterMigrator) getSeeds() ([]string, error) {
	// nodetool getseeds returns seeds other than the current one (seed labeling can't be done here)
	seedsOutput, err := execNodetool(c.NodetoolPath, "getseeeds")
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile(`[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+`)
	seeds := re.FindAllString(seedsOutput, -1)
	sort.Strings(seeds)

	return seeds, nil
}

func (c *ClusterMigrator) CreateSeedServices() error {
	// Get the existing seeds service, if it exists and its endpoints
	// Get the seeds from the current node and apply any new ones to the service

	// Do we need to create additional seeds as well as seeds service? With the first one being updated
	// as the correct ones are matched by seed labels?

	additionalSeedService := &corev1.Service{}
	additionalSeedsKey := types.NamespacedName{Name: c.additionalSeedServiceName(), Namespace: c.Namespace}
	err := c.Client.Get(context.TODO(), additionalSeedsKey, additionalSeedService)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create the service
			if additionalSeedService, err = c.newAdditionalSeedService(); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	seedService := &corev1.Service{}
	seedServiceKey := types.NamespacedName{Name: c.seedServiceName(), Namespace: c.Namespace}
	err = c.Client.Get(context.TODO(), seedServiceKey, seedService)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create the service
			if seedService, err = c.newSeedService(); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// TODO The existing installation should have seeds in the memory / in the cassandra.yaml
	// We need to update the config to use our seed provider

	return nil
}

func (c *ClusterMigrator) CreateClusterConfigMap() error {
	/*
		➜  cassandra git:(trunk) ✗ bin/nodetool gossipinfo
		localhost/127.0.0.1
		generation:1652681225
		heartbeat:104
		STATUS:59:NORMAL,-1742041081749066901
		LOAD:91:106868.0
		SCHEMA:52:54e17321-3f2e-37ca-9b08-d91ba7bdd369
		DC:8:datacenter1
		RACK:10:rack1
		RELEASE_VERSION:5:4.2-SNAPSHOT
		RPC_ADDRESS:4:127.0.0.1
		NET_VERSION:1:12
		HOST_ID:2:da75d97a-d940-43d5-974c-91ee8db8a95e
		RPC_READY:61:true
		NATIVE_ADDRESS_AND_PORT:3:127.0.0.1:9042
		STATUS_WITH_PORT:58:NORMAL,-1742041081749066901
		SSTABLE_VERSIONS:6:big-nb
		TOKENS:57:<hidden>
	*/
	output, err := execNodetool(c.NodetoolPath, "gossipinfo")
	if err != nil {
		return err
	}

	lines := strings.Split(output, "\n")

	detailsStarted := false
	for _, line := range lines {
		if strings.HasPrefix(line, "  ") {
			// Data lines, current node
			if detailsStarted {
				columns := strings.Split(line[2:], ":")
				if len(columns) > 2 {
					fieldName := columns[0]
					fieldValue := columns[2]
					if fieldName == "DC" {
						c.Datacenter = fieldValue
					} else if fieldName == "RACK" {
						c.Rack = fieldValue
					}
				}
			}
		} else if strings.HasPrefix(line, "/") {
			if detailsStarted {
				// We parsed the remaining fields
				break
			}
		} else {
			detailsStarted = true
		}
	}

	// ClusterName
	clusterInfo, err := execNodetool(c.NodetoolPath, "describecluster")
	if err != nil {
		return err
	}

	lines = strings.Split(clusterInfo, "\n")
	fields := strings.Split(lines[1], ":")
	c.Cluster = fields[1][1:]

	fmt.Printf("Parsed the following:\nRack: %s\nDatacenter: %s\nCluster: %s\n", c.Rack, c.Datacenter, c.Cluster)

	return nil
}

func (c *ClusterMigrator) additionalSeedServiceName() string {
	return cassdcapi.CleanupForKubernetes(c.Cluster) + "-" + c.Datacenter + "-additional-seed-service"
}

func (c *ClusterMigrator) seedServiceName() string {
	return cassdcapi.CleanupForKubernetes(c.Cluster) + "-seed-service"
}

func execNodetool(nodetoolPath, command string) (string, error) {
	nodetoolLocation := fmt.Sprintf("%s/nodetool", nodetoolPath)
	out, err := exec.Command(nodetoolLocation, command).Output()
	if err != nil {
		return "", err
	}

	return string(out), err
}

func (c *ClusterMigrator) newSeedService() (*corev1.Service, error) {
	svc := makeHeadlessService(c.seedServiceName(), c.Namespace)
	svc.Spec.Selector = buildLabelSelectorForSeedService(c.Cluster)

	if err := c.Client.Create(context.TODO(), svc); err != nil {
		return nil, err
	}

	return svc, nil
}

func makeHeadlessService(name, namespace string) *corev1.Service {
	var service corev1.Service
	service.ObjectMeta.Name = name
	service.ObjectMeta.Namespace = namespace
	service.Spec.Type = "ClusterIP"
	service.Spec.ClusterIP = "None"
	service.Spec.PublishNotReadyAddresses = true
	return &service
}

func buildLabelSelectorForSeedService(clusterName string) map[string]string {
	return map[string]string{
		"cassandra.datastax.com/cluster":   cassdcapi.CleanupForKubernetes(clusterName),
		"cassandra.datastax.com/seed-node": "true",
	}
}

func (c *ClusterMigrator) newAdditionalSeedService() (*corev1.Service, error) {
	svc := makeHeadlessService(c.additionalSeedServiceName(), c.Namespace)
	if err := c.Client.Create(context.TODO(), svc); err != nil {
		return nil, err
	}

	return svc, nil
}

func (c *ClusterMigrator) endpointsForAdditionalSeeds(seeds []string) (*corev1.Endpoints, error) {

	endpoints := &corev1.Endpoints{}
	endpointsName := types.NamespacedName{Name: c.additionalSeedServiceName(), Namespace: c.Namespace}
	if err := c.Client.Get(context.TODO(), endpointsName, endpoints); err != nil && !errors.IsNotFound(err) {
		return nil, err
	} else if errors.IsNotFound(err) {
		endpoints := corev1.Endpoints{}
		endpoints.ObjectMeta.Name = c.additionalSeedServiceName()
		endpoints.ObjectMeta.Namespace = c.Namespace

		addresses := make([]corev1.EndpointAddress, 0, len(seeds))
		for _, additionalSeed := range seeds {
			if ip := net.ParseIP(additionalSeed); ip != nil {
				addresses = append(addresses, corev1.EndpointAddress{
					IP: additionalSeed,
				})
			}
		}

		// See: https://godoc.org/k8s.io/api/core/v1#Endpoints
		endpoints.Subsets = []corev1.EndpointSubset{
			{
				Addresses: addresses,
			},
		}

		return &endpoints, nil
	}

	// TODO Add the missing seed (the current host perhaps) to the endpoints
	// AddrBlock:
	// for _, addr := range endpoints.Subsets[0].Addresses {
	// 	for _, seed := range seeds {
	// 		if addr.IP == seed {
	// 			continue AddrBlock

	// 		}
	// 	}
	// 	endpoints.Subsets[0].Addresses = append(endpoints.Subsets[0].Addresses, addr.IP)
	// }

	return endpoints, nil
}
