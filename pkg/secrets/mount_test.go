package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVaultMounted(t *testing.T) {
	require := require.New(t)

	tmpDir, err := os.MkdirTemp("", "secret-")
	require.NoError(err)

	tmpFile, err := os.CreateTemp(tmpDir, "users")
	require.NoError(err)

	defer func() {
		tmpFile.Close()
		os.RemoveAll(tmpDir)
	}()

	_, err = tmpFile.WriteString("newuser=password")
	require.NoError(err)

	users, err := readTargetFile(tmpFile.Name())
	require.NoError(err)
	require.Equal(1, len(users))
	require.Contains(users, "newuser")
	require.Equal("password", users["newuser"])
}

func TestSecretMounted(t *testing.T) {
	require := require.New(t)

	tmpDir, err := os.MkdirTemp("", "secret-")
	require.NoError(err)

	tmpFile, err := os.CreateTemp(tmpDir, "user")
	require.NoError(err)

	defer func() {
		tmpFile.Close()
		os.RemoveAll(tmpDir)
	}()

	_, err = tmpFile.WriteString("password")
	require.NoError(err)

	userName := filepath.Base(tmpFile.Name())

	users, err := readTargetSecretMount(tmpDir)
	require.NoError(err)
	require.Equal(1, len(users))
	require.Contains(users, userName)
	require.Equal("password", users[userName])
}
