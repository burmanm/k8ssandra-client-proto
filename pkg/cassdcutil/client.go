package cassdcutil

import (
	"context"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetClient returns a controller-runtime client with cass-operator API defined
func GetClient(restConfig *rest.Config) (client.Client, error) {
	c, err := client.New(restConfig, client.Options{})
	if err != nil {
		return nil, err
	}

	err = cassdcapi.AddToScheme(c.Scheme())

	return c, err
}

func GetClientInNamespace(restConfig *rest.Config, namespace string) (client.Client, error) {
	c, err := GetClient(restConfig)
	if err != nil {
		return nil, err
	}

	c = client.NewNamespacedClient(c, namespace)
	return c, nil
}

func CreateNamespaceIfNotExists(client client.Client, namespace string) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}

	if err := client.Create(context.TODO(), ns); err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return nil
}
