package editor

import (
	"os"
	"os/exec"
)

const (
	defaultEditor = "nano"
)

// OpenEditor opens the editor to modify file X
func OpenEditor(filename string) error {
	// What about $VISUAL ?
	editor := os.Getenv("EDITOR")

	if editor == "" {
		editor = defaultEditor
	}

	targetExec, err := exec.LookPath(editor)
	if err != nil {
		return err
	}

	// This will only work with command line editors, not if EDITOR spawns a visual editor..
	cmd := exec.Command(targetExec, filename)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
