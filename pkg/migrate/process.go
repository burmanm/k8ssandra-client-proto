package migrate

/*
	Process:
		* Get currentNode
		* Fetch rackName, clusterName, datacenterName, ordinal, etc from the ConfigMaps we created previously
		* drain+shutdown current node
		* Parse config for data directories
		* Create PVC + PV with the storage information
		* Parse configuration
		* Create Pod
		* Wait for the pod to finish starting
		* Wait until the node shows up correctly again in the cluster
*/
