package migrate

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigParsing(t *testing.T) {
	require := require.New(t)
	confDir := filepath.Join("..", "..", "testfiles")
	parser := NewParser()
	err := parser.ParseConfigDirectories(confDir, confDir, "")
	require.NoError(err)

	err = parser.ParseConfigs()

	require.Equal(4, len(parser.yamls))
	require.NoError(err)

	require.Equal(1, len(parser.yamls["jvm11-server-options"]))
	addJvmOptions := parser.yamls["jvm11-server-options"]["additional-jvm-options"].([]string)
	require.Equal(12, len(addJvmOptions))
}

func TestParseDataPaths(t *testing.T) {
	require := require.New(t)
	confDir := filepath.Join("..", "..", "testfiles")
	parser := NewParser()
	err := parser.ParseConfigDirectories(confDir, confDir, "")
	require.NoError(err)

	err = parser.ParseConfigs()
	require.NoError(err)

	dataDirs, additionalDirs, err := parseDataPaths(parser.CassYaml())
	require.NoError(err)

	// One data_file_directories path, one commitlog_directory path
	require.Equal(1, len(additionalDirs))
	require.Equal(1, len(dataDirs))
}

/*
	TODO: Could configs be located in multiple directories?
		  Especially the extra workload ones..
*/
