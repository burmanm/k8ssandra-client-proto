package util

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SecretUtil providers some helper methods to fetch the correct secrets required to issue
// the authentication to command
type SecretUtil struct {
	Client client.Client
}

/*
	Some utilities for exec to a pod, basically a wrapper around kubectl's exec

	- Or - for a cluster

	- Fetch secret to connect to the CassandraDatacenter (whichever is used - JMX or superuser)

	- Fetch target CassandraDatacenter and the Secret key(s) associated with it
	  - Can I fetch the helm properties somehow to verify what secret I do have (the describe does not have that info)
*/

func (s *SecretUtil) GetCassandraAuthCredentials(cluster string) (*corev1.Secret, error) {
	return s.authCredentials(secret, cassAuthEnvUsernameName, cassAuthEnvPasswordName)
}

func (s *SecretUtil) GetJmxAuthCredentials(cluster string) (*corev1.Secret, error) {
	// TODO Do I need per service? Like get reaper's JMX credentials?
	return s.authCredentials(secret, jmxAuthEnvUsernameName, jmxAuthEnvPasswordName)
}

func (s *SecretUtil) getSecret(ctx context.Context, key types.NamespacedName) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	err := s.Client.Get(ctx, key, secret)

	return secret, err
}
