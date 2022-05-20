package migrate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pterm/pterm"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// type ConfigParser struct {
// 	client.Client
// 	cassandraHome string
// 	datacenter    string
// 	namespace     string
// }

var serverOptionName = regexp.MustCompile("^jvm.*-server.options$")

// func NewParser(client client.Client, namespace, cassandraHome, datacenter string) *ConfigParser {
// 	return &ConfigParser{
// 		Client:        client,
// 		cassandraHome: cassandraHome,
// 		datacenter:    datacenter,
// 		namespace:     namespace,
// 	}
// }

func (c *ClusterMigrator) ParseConfigs(p *pterm.SpinnerPrinter) error {
	confMap, err := c.getOrCreateConfigMap()
	if err != nil {
		return err
	}

	p.UpdateText("Parsing all JVM options files")
	confMap, err = c.fetchAllOptionFiles(confMap)
	if err != nil {
		return err
	}
	pterm.Success.Println("Stored all JVM options to Kubernetes")

	p.UpdateText("Parsing cassandra.yaml")
	_, err = c.parseCassandraYaml(confMap)
	if err != nil {
		return err
	}
	pterm.Success.Println("Parsed and stored cassandra.yaml to Kubernetes")

	return nil
}

func (c *ClusterMigrator) getOrCreateConfigMap() (*corev1.ConfigMap, error) {
	configFilesMap := &corev1.ConfigMap{}
	configFilesMapKey := types.NamespacedName{Name: getConfigMapName(c.Datacenter, "cass-config"), Namespace: c.Namespace}
	if err := c.Client.Get(context.TODO(), configFilesMapKey, configFilesMap); err != nil && !errors.IsNotFound(err) {
		return nil, err
	} else if errors.IsNotFound(err) {
		configFilesMap.ObjectMeta.Name = configFilesMapKey.Name
		configFilesMap.ObjectMeta.Namespace = configFilesMapKey.Namespace
		configFilesMap.Data = map[string]string{}
		if err := c.Client.Create(context.TODO(), configFilesMap); err != nil {
			return nil, err
		}
	}

	return configFilesMap, nil
}

func (c *ClusterMigrator) fetchAllOptionFiles(configFilesMap *corev1.ConfigMap) (*corev1.ConfigMap, error) {

	/*
		// Parse through all $CONF_DIRECTORY/jvm*-server.options and write them to a ConfigMap
		filepath.WalkDir(c.getConfigDir(), func(path string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				// We're not processing subdirs
				return nil
			}
			if serverOptionName.MatchString(d.Name()) {
				// Parse this file and add it to the ConfigMap
				f, err := os.Open(path)
				if err != nil {
					return err
				}

				defer f.Close()

				var configData strings.Builder

				// Remove the comment lines to reduce the ConfigMap size
				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					line := scanner.Text()
					if !strings.HasPrefix(line, "#") {
						configData.WriteString(line)
					}
				}

				if err := scanner.Err(); err != nil {
					return err
				}

				keyName := strings.ReplaceAll(d.Name(), ".", "-")
				configFilesMap.Data[keyName] = configData.String()
			}
			return nil
		})
	*/
	return configFilesMap, nil
}

func (c *ClusterMigrator) parseCassandraYaml(configFilesMap *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	// Parse the $CONF_DIRECTORY/cassandra.yaml
	yamlPath := filepath.Join(c.getConfigDir(), "cassandra.yaml")
	yamlFile, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, err
	}

	if configFilesMap.Data == nil {
		configFilesMap.Data = make(map[string]string)
	}

	// Unmarshal, Marshal to remove all comments (and some fields if necessary)
	target := make(map[string]interface{})

	if err := yaml.Unmarshal(yamlFile, target); err != nil {
		return nil, err
	}

	/*
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

	// Parse seeds
	if seedProviders, ok := target["seed_provider"].([]interface{}); ok {
		for _, seedProvider := range seedProviders {
			if seedProv, ok := seedProvider.(map[string]interface{}); ok {
				if params, found := seedProv["parameters"]; found {
					if paramsSlice, ok := params.([]interface{}); ok {
						for _, partSlice := range paramsSlice {
							if castSlice, ok := partSlice.(map[string]interface{}); ok {
								if seedList, found := castSlice["seeds"]; found {
									seeds := strings.Split(seedList.(string), ",")
									for _, seed := range seeds {
										seedAddr := strings.Split(seed, ":")
										if seedAddr[0] != "127.0.0.1" {
											// Loopback isn't allowed endpoint value in Kubernetes
											c.seeds = append(c.seeds, seedAddr[0])
										}
									}
								}
							}
						}
					}
				}
			}
		}

	}

	// We could read some details here also instead of getseeds etc earlier..
	delete(target, "seed_provider")

	out, err := yaml.Marshal(target)
	if err != nil {
		return nil, err
	}

	// configFilesMap := &corev1.ConfigMap{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name: getConfigMapName(c.datacenter, "cass-config"),
	// 	},
	// 	Data: map[string]string{
	// 		"cassandra.yaml": string(out),
	// 	},
	// }

	configFilesMap.Data["cassandra-yaml"] = string(out)

	if err := c.Client.Update(context.TODO(), configFilesMap); err != nil {
		return nil, err
	}

	return configFilesMap, nil
}

func getConfigMapName(datacenter, configName string) string {
	return fmt.Sprintf("%s-%s", datacenter, configName)
}

func (c *ClusterMigrator) getConfigDir() string {
	// TODO Give the possibility to override config path
	return fmt.Sprintf("%s/conf", c.CassandraHome)
}
