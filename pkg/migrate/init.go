package migrate

/*
	Init:
		* nodetool getseeds
			* Create ConfigMap to indicate host/UUID -> isSeed
			* Write the seeds-service with seed IPs
		* nodetool status / describecluster / etc:
			* Create ConfigMap with cluster knowledge:
				* hostUUID -> ordinal
				* serverType
				* serverVersion
				* clusterName
				* datacenterName
				* hostUUID -> rackName
*/
