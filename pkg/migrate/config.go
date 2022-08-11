package migrate

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
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

type ConfigParser struct {
	configDir string
	// cassandraHome string

	yamls map[string]map[string]interface{}

	jvmOptions map[string]string
}

func (p *ConfigParser) Yamls() map[string]map[string]interface{} {
	return p.yamls
}

func (p *ConfigParser) CassYaml() map[string]interface{} {
	return p.yamls["cassandra-yaml"]
}

func (p *ConfigParser) JvmOptions(jdkVersion string) string {
	keyName := p.getJvmOptionsKey(jdkVersion)
	return p.jvmOptions[keyName]
}

func (p *ConfigParser) ParseConfigs() error {
	if err := p.parseYaml("cassandra.yaml"); err != nil {
		return err
	}

	if err := p.parseYaml("dse.yaml"); err != nil {
		return err
	}

	if err := p.parseJVMOptions(); err != nil {
		return err
	}

	return nil
}

var serverOptionName = regexp.MustCompile("^jvm.*-server.options$")

func NewParser(configDir string) *ConfigParser {
	return &ConfigParser{
		configDir:  configDir,
		jvmOptions: make(map[string]string),
		yamls:      make(map[string]map[string]interface{}),
	}
}

func (c *ClusterMigrator) ParseConfigs(p *pterm.SpinnerPrinter) error {
	cfgParser := NewParser(c.ConfigDir)
	if err := cfgParser.ParseConfigs(); err != nil {
		return err
	}

	confMap, err := c.getOrCreateConfigMap()
	if err != nil {
		return err
	}

	p.UpdateText("Parsing all Cassandra configuration files")
	err = cfgParser.ParseConfigs()
	if err != nil {
		return err
	}

	p.UpdateText("Storing configs to Kubernetes")
	_, err = c.storeConfigFiles(confMap, cfgParser.Yamls())
	if err != nil {
		return err
	}
	pterm.Success.Println("Parsed and stored Cassandra configuration files to Kubernetes")

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

func (p *ConfigParser) parseJVMOptions() error {
	// Parse through all $CONF_DIRECTORY/jvm*-server.options and write them to a ConfigMap
	return filepath.WalkDir(p.getConfigDir(), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Couldn't access the file for some reason
			return err
		}

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
			p.jvmOptions[keyName] = configData.String()
		}
		return nil
	})
}

func (p *ConfigParser) getJvmOptionsKey(jdkVersion string) string {
	return strings.ReplaceAll(fmt.Sprintf("jvm%s-server.options", jdkVersion), ".", "-")
}

func (p *ConfigParser) parseYaml(name string) error {
	yamlPath := filepath.Join(p.getConfigDir(), name)

	if _, err := os.Stat(yamlPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	yamlFile, err := os.ReadFile(yamlPath)
	if err != nil {
		return err
	}

	// Unmarshal, Marshal to remove all comments (and some fields if necessary)
	target := make(map[string]interface{})

	if err := yaml.Unmarshal(yamlFile, target); err != nil {
		return err
	}

	modifiedName := strings.ReplaceAll(name, ".", "-")
	p.yamls[modifiedName] = target
	return nil
}

func (c *ClusterMigrator) storeConfigFiles(configFilesMap *corev1.ConfigMap, yamls map[string]map[string]interface{}) (*corev1.ConfigMap, error) {
	if configFilesMap.Data == nil {
		configFilesMap.Data = make(map[string]string)
	}

	for name, yamlConf := range yamls {
		if name == "cassandra-yaml" {
			// Parse seeds
			if seedProviders, ok := yamlConf["seed_provider"].([]interface{}); ok {
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

			// These keys are not used in the Kubernetes installation
			delete(yamlConf, "seed_provider")
			delete(yamlConf, "listen_address")
			delete(yamlConf, "listen_interface")
		}
		out, err := yaml.Marshal(yamlConf)
		if err != nil {
			return nil, err
		}

		// cass-config-builder uses "cassandra-yaml" and "dse-yaml"
		configFilesMap.Data[name] = string(out)
	}

	if err := c.Client.Update(context.TODO(), configFilesMap); err != nil {
		return nil, err
	}

	return configFilesMap, nil
}

func getConfigMapName(datacenter, configName string) string {
	return fmt.Sprintf("%s-%s", datacenter, configName)
}

func (p *ConfigParser) getConfigDir() string {
	// TODO Give the possibility to override config path
	return p.configDir
	// return fmt.Sprintf("%s/conf", p.cassandraHome)
}
