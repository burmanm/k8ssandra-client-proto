package cassdcutil

import (
	"context"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	return c, err
}

func GetClientInNamespace(namespace string) (client.Client, error) {
	c, err := GetClient()
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
