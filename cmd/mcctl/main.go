package main

import (
	"os"

	"github.com/spf13/pflag"

	"github.com/burmanm/k8ssandra-client/cmd/mcctl/mc"

	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func main() {
	flags := pflag.NewFlagSet("mcctl", pflag.ExitOnError)
	pflag.CommandLine = flags

	root := mc.NewCmd(genericclioptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr})
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
