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

	_, err = tmpFile.WriteString("newuser=password====")
	require.NoError(err)

	users, err := readTargetFile(tmpFile.Name())
	require.NoError(err)
	require.Equal(1, len(users))
	require.Contains(users, "newuser")
	require.Equal("password====", users["newuser"])
}

func TestSecretMounted(t *testing.T) {
	require := require.New(t)

	tmpDir, err := os.MkdirTemp("", "secret-")
	require.NoError(err)

	username := "superuser"
	password := "superpassword"

	err = os.WriteFile(filepath.Join(tmpDir, "username"), []byte(username), 0644)
	require.NoError(err)

	err = os.WriteFile(filepath.Join(tmpDir, "password"), []byte(password), 0644)
	require.NoError(err)

	defer func() {
		os.RemoveAll(tmpDir)
	}()

	users, err := readTargetSecretMount(tmpDir)
	require.NoError(err)
	require.Equal(1, len(users))
	require.Contains(users, username)
	require.Equal(password, users[username])
}
