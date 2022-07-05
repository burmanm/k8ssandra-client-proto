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

// readTargetSecretMount is processing the old standard set by cass-operator
// this method can only parse a single username/password pair
func readTargetSecretMount(path string) (map[string]string, error) {
	var username, password string

	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Couldn't access the file for some reason
			return err
		}

		if d.IsDir() {
			// This will be walked later
			return nil
		}

		// We should have two keys here: username and password and use that information..
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		defer f.Close()

		fileContents, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		data := string(fileContents)

		switch d.Name() {
		case "password":
			password = data
		case "username":
			username = data
		}

		return nil
	})

	return map[string]string{username: password}, err
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
		userInfo := strings.SplitN(line, "=", 2)
		if len(userInfo) > 1 {
			users[userInfo[0]] = userInfo[1]
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return users, nil
}
