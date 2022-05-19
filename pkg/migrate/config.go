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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigParser struct {
	client.Client
	cassandraHome string
	datacenter    string
	namespace     string
}

var serverOptionName = regexp.MustCompile("^jvm.*-server.options$")

func NewParser(client client.Client, namespace, cassandraHome, datacenter string) *ConfigParser {
	return &ConfigParser{
		Client:        client,
		cassandraHome: cassandraHome,
		datacenter:    datacenter,
		namespace:     namespace,
	}
}

func (c *ConfigParser) ParseConfigs(p *pterm.SpinnerPrinter) error {
	p.UpdateText("Fetching all JVM options")
	confMap, err := c.fetchAllOptionFiles()
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

func (c *ConfigParser) fetchAllOptionFiles() (*corev1.ConfigMap, error) {
	configFilesMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: getConfigMapName(c.datacenter, "cass-config"),
		},
		Data: make(map[string]string),
	}

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

	if err := c.Client.Create(context.TODO(), configFilesMap); err != nil {
		return nil, err
	}
	return configFilesMap, nil
}

func (c *ConfigParser) parseCassandraYaml(configFilesMap *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	// Parse the $CONF_DIRECTORY/cassandra.yaml
	yamlPath := filepath.Join(c.getConfigDir(), "cassandra.yaml")
	yamlFile, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, err
	}

	// Unmarshal, Marshal to remove all comments (and some fields if necessary)
	target := make(map[string]interface{})

	if err := yaml.Unmarshal(yamlFile, target); err != nil {
		return nil, err
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

func (c *ConfigParser) getConfigDir() string {
	// TODO Give the possibility to override config path
	return fmt.Sprintf("%s/conf", c.cassandraHome)
}
