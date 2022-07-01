package secrets

import (
	"bufio"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// readTargetPath supports two different formats for users. If the target is a file, it must be in the format
// username=password, if it's a directory, then it must follow the Kubernetes secret format,
// filename = username, file = password
func ReadTargetPath(path string) (map[string]string, error) {
	f, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if f.IsDir() {
		return readTargetSecretMount(path)
	}
	return readTargetFile(path)
}

func readTargetSecretMount(path string) (map[string]string, error) {
	users := make(map[string]string)
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			// We're not processing subdirs
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		defer f.Close()

		fileContents, err := io.ReadAll(f)
		users[d.Name()] = string(fileContents)

		return nil
	})

	return users, err
}

func readTargetFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	users := make(map[string]string)

	// Remove the comment lines to reduce the ConfigMap size
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// TODO This isn't correct way, we need to let users have "=" on the password also
		userInfo := strings.Split(line, "=")
		if len(userInfo) > 1 {
			users[userInfo[0]] = userInfo[1]
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return users, nil
}
