package cassdcutil

import (
	"log"

	cassdcapi "github.com/datastax/cass-operator/operator/pkg/apis/cassandra/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetClient returns a controller-runtime client with cass-operator API defined
func GetClient() (client.Client, error) {
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{})
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	err = cassdcapi.AddToScheme(c.Scheme())

	return c, nil
}
