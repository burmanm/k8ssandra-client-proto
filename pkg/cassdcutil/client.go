package cassdcutil

import (
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetClient returns a controller-runtime client with cass-operator API defined
func GetClient() (client.Client, error) {
	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, err
	}

	err = cassdcapi.AddToScheme(c.Scheme())

	return c, nil
}

func GetClientInNamespace(namespace string) (client.Client, error) {
	c, err := GetClient()
	if err != nil {
		return nil, err
	}

	c = client.NewNamespacedClient(c, namespace)
	return c, nil
}
