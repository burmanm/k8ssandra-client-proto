package migrate

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	installerDefaultNodetoolPath = "/usr/share/dse/cassandra/tools/nodetool"
)

// DetectInstallation tries to find the nodetool from the given paths
func DetectInstallation(cassandraHome, nodetoolPath string) (string, string, error) {
	if cassandraHome != "" {
		// CassandraHome was overridden, use it
		found, err := VerifyFileExists(cassandraHome)
		if err != nil {
			return "", "", err
		}
		if !found {
			return "", "", fmt.Errorf("cassandra-home is invalid")
		}
	}

	if cassandraHome == "" {
		dseHome := os.Getenv("DSE_HOME")
		if dseHome != "" {
			found, err := VerifyFileExists(dseHome)
			if err != nil {
				return "", "", err
			}
			if !found {
				return "", "", fmt.Errorf("DSE_HOME env variable is invalid")
			}
			cassandraHome = dseHome
		}
	}

	// No overrides used, check the default paths - if --nodetool-path was used, this won't override it
	nodetoolSetter := func(path string) error {
		if nodetoolPath == "" {
			found, err := VerifyFileExists(path)
			if err != nil {
				return err
			}
			if found {
				nodetoolPath = path
			}
		}
		return nil
	}

	if cassandraHome == "" {
		// Check package installation
		if err := nodetoolSetter(installerDefaultNodetoolPath); err != nil {
			return "", "", err
		}
	}

	if cassandraHome != "" {
		// Check nodetool path, depending on how the user gave us the cassandra-home
		dseRootNodetoolPath := filepath.Join(cassandraHome, "resources", "cassandra", "bin", "nodetool")
		if err := nodetoolSetter(dseRootNodetoolPath); err != nil {
			return "", "", err
		}

		// If the given path was in the form with resources/cassandra included or other CASSANDRA_HOME
		cassandraHomeNodetoolPath := filepath.Join(cassandraHome, "bin", "nodetool")
		if err := nodetoolSetter(cassandraHomeNodetoolPath); err != nil {
			return "", "", err
		}
	}

	if nodetoolPath == "" {
		// We failed
		return "", "", fmt.Errorf("unable to detect correct location of nodetool")
	}

	return cassandraHome, nodetoolPath, nil
}
