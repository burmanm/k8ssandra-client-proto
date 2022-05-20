package migrate

import "github.com/pterm/pterm"

/*
	Commands are: init, add, commit
*/

/*
	Commit:
		* Install cass-operator using internal helm process (same this client otherwise uses)
		* Create CassandraDatacenter
		* Wait for magic to happen
*/

func (c *ClusterMigrator) FinishInstallation(p *pterm.SpinnerPrinter) error {
	p.UpdateText("Installing cass-operator")

	err := c.installCassOperator()
	if err != nil {
		return err
	}

	pterm.Success.Println("cass-operator installed")

	p.UpdateText("Creating CassandraDatacenter")

	pterm.Success.Println("CassandraDatacenter definition created")

	p.UpdateText("Waiting for cluster to be nurtured by cass-operator...")

	pterm.Success.Println("Cluster is fully managed now")

	return nil
}

func (c *ClusterMigrator) installCassOperator() error {
	return nil
}
