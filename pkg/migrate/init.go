package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

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
	NodetoolPath  string
	CassandraHome string

	// TODO Merge ClusterMigrator and NodeMigrator?

	Cluster    string
	Datacenter string
	Rack       string

	KubeNode  string
	Ordinal   int
	Namespace string

	ServerType    string
	ServerVersion string
}

func NewClusterMigrator(client client.Client, namespace, cassandraHome string) (*ClusterMigrator, error) {
	return &ClusterMigrator{
		Client:        client,
		Namespace:     namespace,
		CassandraHome: cassandraHome,
	}, nil
}

func (c *ClusterMigrator) InitCluster(p *pterm.SpinnerPrinter) error {
	// p, err := pterm.DefaultProgressbar.WithTitle("Parsing cluster details").WithShowCount(false).WithShowPercentage(false).Start()
	// if err != nil {
	// 	return err
	// }

	// TODO Replace with BubbleTea

	p.UpdateText("Fetching cluster details")
	err := c.CreateClusterConfigMap()
	if err != nil {
		// fmt.Printf("Failed to get cluster details: %v\n", err)
		// pterm.Fatal.Println("Failed to get cluster details")
		return err
	}

	pterm.Success.Println("Fetched cluster details and stored them to Kubernetes")

	p.UpdateText("Fetching seeds")
	err = c.CreateSeedServices()
	if err != nil {
		// fmt.Printf("Failed to get cluster seeds: %v\n", err)
		// pterm.Fatal.Println("Failed to get cluster seeds")
		return err
	}
	pterm.Success.Println("Created seed services")

	// pterm.Info.Println("You can now import nodes to the Kubernetes")

	// TODO After nodeMigrations:

	// Create CassandraDatacenter with the known data + calculate how many nodes as size

	return nil
}

func (c *ClusterMigrator) getSeeds() ([]string, error) {
	// nodetool getseeds returns seeds other than the current one (seed labeling can't be done here)
	seedsOutput, err := execNodetool(c.getNodetoolPath(), "getseeds")
	if err != nil {
		return nil, err
	}

	/*
		TODO Parse from cassandra.yaml:

		seed_provider:
		  # Addresses of hosts that are deemed contact points.
		  # Cassandra nodes use this list of hosts to find each other and learn
		  # the topology of the ring.  You must change this if you are running
		  # multiple nodes!
		  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
		    parameters:
		      # seeds is actually a comma-delimited list of addresses.
		      # Ex: "<ip1>,<ip2>,<ip3>"
		      - seeds: "127.0.0.1:7000"
	*/

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
			if _, err = c.newAdditionalSeedService(); err != nil {
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
			if _, err = c.newSeedService(); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	seeds, err := c.getSeeds()
	if err != nil {
		return err
	}

	// TODO Verify endpoints is updated with all the possible seeds
	if len(seeds) > 0 {
		_, err := c.endpointsForAdditionalSeeds(seeds)
		if err != nil {
			return err
		}
	}

	// TODO The existing installation should have seeds in the memory / in the cassandra.yaml
	// We need to update the config to use our seed provider

	return nil
}

func (c *ClusterMigrator) CreateClusterConfigMap() error {
	// TODO Or should we use nodetool info first and then just find the correct one?
	output, err := execNodetool(c.getNodetoolPath(), "gossipinfo")
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
					switch fieldName {
					case "DC":
						c.Datacenter = fieldValue
					case "RACK":
						c.Rack = fieldValue
					case "RELEASE_VERSION":
						if c.ServerType == "" {
							// We haven't parsed DSE information yet, so we can safely parse this
							c.ServerType = "cassandra"
							c.ServerVersion = "4.0.3"
							// My local setup is using a snapshot version and config-builder does not support it
							// c.ServerVersion = fieldValue
						}
					case "X_11_PADDING":
						// DSE 6.8
						dseInfo := make(map[string]string)
						err = json.Unmarshal([]byte(fieldValue), &dseInfo)
						if err != nil {
							return err
						}
						c.ServerType = "dse"
						c.ServerVersion = dseInfo["dse_version"]
						// We could parse graph / search / etc settings here also for DSE
					}
				}
			}
		} else if strings.HasPrefix(line, "/") {
			if detailsStarted {
				// We parsed the remaining fields, this is starting next node
				break
			}
		} else {
			detailsStarted = true
		}
	}

	// ClusterName
	clusterInfo, err := execNodetool(c.getNodetoolPath(), "describecluster")
	if err != nil {
		return err
	}

	lines = strings.Split(clusterInfo, "\n")
	fields := strings.Split(lines[1], ":")
	c.Cluster = fields[1][1:]

	configMap := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{Name: configMapName(c.Datacenter), Namespace: c.Namespace}
	if err := c.Client.Get(context.TODO(), configMapKey, configMap); err != nil && !errors.IsNotFound(err) {
		return err
	} else if errors.IsNotFound(err) {
		nodeInfos, err := c.retrieveStatusFromNodetool()
		if err != nil {
			return err
		}

		configMap.ObjectMeta.Name = configMapName(c.Datacenter)
		configMap.ObjectMeta.Namespace = c.Namespace
		infoMap := map[string]string{
			"cluster":       c.Cluster,
			"serverVersion": c.ServerVersion,
			"serverType":    c.ServerType,
		}
		i := 0
		// Create ordinal information for the next stages
		for _, nodeInfo := range nodeInfos {
			infoMap[nodeInfo.HostId] = strconv.Itoa(i)
		}
		configMap.Data = infoMap
		if err := c.Client.Create(context.TODO(), configMap); err != nil {
			return err
		}
	}

	return nil
}

func configMapName(datacenter string) string {
	return fmt.Sprintf("%s-migrate-config", cassdcapi.CleanupForKubernetes(datacenter))
}

func (c *ClusterMigrator) additionalSeedServiceName() string {
	return cassdcapi.CleanupForKubernetes(c.Cluster) + "-" + c.Datacenter + "-additional-seed-service"
}

func (c *ClusterMigrator) seedServiceName() string {
	return cassdcapi.CleanupForKubernetes(c.Cluster) + "-seed-service"
}

func (c *ClusterMigrator) getNodetoolPath() string {
	if c.NodetoolPath != "" {
		return c.NodetoolPath
	}
	return fmt.Sprintf("%s/bin", c.CassandraHome)
}

func execNodetool(nodetoolPath, command string) (string, error) {
	nodetoolLocation := fmt.Sprintf("%s/nodetool", nodetoolPath)
	out, err := exec.Command(nodetoolLocation, command).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			if ee.ExitCode() == 1 {
				return "", fmt.Errorf("unable to execute nodetool against localhost")
			}
		}
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

		if err = c.Client.Create(context.TODO(), &endpoints); err != nil {
			return nil, err
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

type NodetoolNodeInfo struct {
	Status  string
	State   string
	Address string
	HostId  string
	Rack    string
}

// From cass-operator tests
func (c *ClusterMigrator) retrieveStatusFromNodetool() ([]NodetoolNodeInfo, error) {
	output, err := execNodetool(c.getNodetoolPath(), "status")
	if err != nil {
		return nil, err
	}

	getFullName := func(s string) string {
		status, ok := map[string]string{
			"U": "up",
			"D": "down",
			"N": "normal",
			"L": "leaving",
			"J": "joining",
			"M": "moving",
			"S": "stopped",
		}[string(s)]

		if !ok {
			status = s
		}
		return status
	}

	nodeTexts := regexp.MustCompile(`(?m)^.*(([0-9a-fA-F]+-){4}([0-9a-fA-F]+)).*$`).FindAllString(output, -1)
	nodeInfo := []NodetoolNodeInfo{}
	for _, nodeText := range nodeTexts {
		comps := regexp.MustCompile(`[[:space:]]+`).Split(strings.TrimSpace(nodeText), -1)
		nodeInfo = append(nodeInfo,
			NodetoolNodeInfo{
				Status:  getFullName(string(comps[0][0])),
				State:   getFullName(string(comps[0][1])),
				Address: comps[1],
				HostId:  comps[len(comps)-2],
				Rack:    comps[len(comps)-1],
			})
	}
	return nodeInfo, nil
}
