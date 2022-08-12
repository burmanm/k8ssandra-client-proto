package migrate

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigParsing(t *testing.T) {
	require := require.New(t)
	confDir := filepath.Join("..", "..", "testfiles")
	parser := NewParser(confDir)
	err := parser.ParseConfigs()
	require.NoError(err)
}

func TestParseDataPaths(t *testing.T) {
	require := require.New(t)
	confDir := filepath.Join("..", "..", "testfiles")
	parser := NewParser(confDir)
	err := parser.ParseConfigs()
	require.NoError(err)

	dataDirs, additionalDirs, err := parseDataPaths(parser.CassYaml())
	require.NoError(err)

	// One data_file_directories path, one commitlog_directory path
	require.Equal(1, len(additionalDirs))
	require.Equal(1, len(dataDirs))
}
