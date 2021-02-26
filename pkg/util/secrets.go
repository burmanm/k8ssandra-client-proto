package util

import (
	"context"
	"fmt"

	"github.com/burmanm/k8ssandra-client/pkg/cassdcutil"
	cassdcapi "github.com/datastax/cass-operator/operator/pkg/apis/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type CassandraPodSecrets struct {
	Username string
	Password string
}

func GetCassandraSuperuserSecrets(podName, namespace string) (*CassandraPodSecrets, error) {
	c, err := cassdcutil.GetClient()
	if err != nil {
		return nil, err
	}

	key := types.NamespacedName{Namespace: namespace, Name: podName}
	pod := &corev1.Pod{}
	err = c.Get(context.TODO(), key, pod)
	if err != nil {
		return nil, err
	}

	if dc, found := pod.Labels[cassdcapi.DatacenterLabel]; !found {
		return nil, fmt.Errorf("Target pod not managed by k8ssandra, no datacenter label")
	} else {
		// Get CassandraDatacenter for the dc
		cassDcKey := types.NamespacedName{Namespace: namespace, Name: dc}
		cassdc := &cassdcapi.CassandraDatacenter{}
		err = c.Get(context.TODO(), cassDcKey, cassdc)
		if err != nil {
			return nil, err
		}

		secret := &corev1.Secret{}
		err = c.Get(context.TODO(), cassdc.GetSuperuserSecretNamespacedName(), secret)

		return &CassandraPodSecrets{
			Username: string(secret.Data["username"]),
			Password: string(secret.Data["password"]),
		}, nil
	}
}
