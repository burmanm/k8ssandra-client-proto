/*
	kubectl k8ssandra command <node>/<cluster>/<release> <parameters> <flags>
	If no node / cluster / release is required, then the parameters is after command

	command selection, such as:
	|remove <release> 				=> uninstall CassandraDatacenter (+ finalizers, + unused secrets?) for release X, but nothing else (cleaner)
	|stop <cluster>					=> shutdown cluster X (but do not delete it)
	|start <cluster>				=> resume operation of shutdown cluster (should this be suspend / resume ?)
	|nodetool <node>				=> use nodetool on node X
	|restart <cluster>				=> issue rolling restart for cluster X
	|cqlsh <node>					=> exec cqlsh in the node
	backup 							=> fetch backup information? *
	restore <cluster> <backup>		=> initiate a restore of cluster X to a version Y. What about to a new cluster?
	repair <cluster>				=> repair cluster X now
	get all							=> get all k8ssandra resources (more than kubectl get all)
	|upgradecrds					=> upgrade installed CRDs to the newest versions from k8ssandra
	|edit release <release>			=> edit settings of k8ssandra release
	edit cassdc <cassdc>			=> edit CassandraDatacenter with comments on the YAML
	init							=> install all operators, but not any Cassandra cluster
									   helm install release charts/k8ssandra --set cassandra.enabled=false
									   or install only cass-operator, nothing else? And allow parameters to install more / use components to install more
	install 						=> install Cassandra cluster, present config editor and auto-create releaseName (cluster-<random>)
	|*list							=> get all the installations in the target k8s cluster(s) - list?
									   also display if there's an update available
	cleancache						=> remove cached Helm releases
	upgrade							=> upgrade k8ssandra version
	clientstatus					=> show cache size, show newest available versions (stable + devel?)
	status							=> show detailed status of installed clusters (like list, but with more details such as fetced from nodetool status)
	port-forward <service>			=> open port-forward to Reaper UI, Grafana UI, etc

	components						=> install, list, uninstall, edit current components (stargate, reaper, medusa..)

	import							=> import existing Cassandra/DSE installation to k8ssandra

	*
		* backup list <cluster>
		* backup create <cluster>
		* backup create schedule? (once we implement such - although with CronJob one could run this to get around this behavior)
		* backup status (and restore status), show current backup processes status, same for repair and restore
	--wait (wait for some process to finish)

	* Should we have tools under which "nodetool" and "cqlsh" would live in?

	* If upgrade is initiated (version of underneath Helm chart), remember to run necessary hooks such as CRD updates

	| indicates the feature is implemented (at least partially). This TODO list is not indication of correct parameters, verify from the command itself
*/
package main

import (
	"os"

	"github.com/spf13/pflag"

	"github.com/burmanm/k8ssandra-client/cmd/kubectl-k8ssandra/k8ssandra"

	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func main() {
	flags := pflag.NewFlagSet("kubectl-k8ssandra", pflag.ExitOnError)
	pflag.CommandLine = flags

	root := k8ssandra.NewCmd(genericclioptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr})
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
