package migrate

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGidParsing(t *testing.T) {
	require := require.New(t)
	tempDir, err := os.MkdirTemp("", "test-")
	require.NoError(err)

	userGid := os.Getgid()
	gid, err := GetFsGroup(tempDir)
	require.NoError(err)
	require.Equal(userGid, int(gid))

	require.NoError(os.RemoveAll(tempDir))
}

func TestGroupWriteChanges(t *testing.T) {
	require := require.New(t)
	tempDir, err := os.MkdirTemp("", "test-")
	require.NoError(err)

	require.NoError(os.Chmod(tempDir, 0600))

	fsInfoOrig, err := os.Stat(tempDir)
	require.NoError(err)

	require.Equal(fs.FileMode(0o600), fsInfoOrig.Mode().Perm())

	require.NoError(FixDirectoryRights(tempDir))

	fsInfoOrig, err = os.Stat(tempDir)
	require.NoError(err)

	require.Equal(fs.FileMode(0o660), fsInfoOrig.Mode().Perm())

	require.NoError(os.RemoveAll(tempDir))
}
