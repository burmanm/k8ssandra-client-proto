package util

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type CassandraPodSecrets struct {
	CassandraSuperuserSecret string
	CassandraJMXSecret       string
}

func GetCassandraSuperuserNameAndPassword(secretClient coreclient.SecretsGetter, namespace, releaseName string) (string, string, error) {
	secret, err := secretClient.Secrets(namespace).Get(context.TODO(), "demo-reaper-secret-k8ssandra", metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	user := secret.Data["username"]
	pass := secret.Data["password"]

	return string(user), string(pass), nil
}

func GetJmxUserNamePassword(secretClient coreclient.SecretsGetter, namespace, releaseName string) (string, string, error) {
	// TODO Hack for now, this requires a patch in k8ssandra
	secret, err := secretClient.Secrets(namespace).Get(context.TODO(), "demo-reaper-secret-k8ssandra", metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	user := secret.Data["username"]
	pass := secret.Data["password"]

	return string(user), string(pass), nil
}

func GetPodSecrets(podClient coreclient.PodsGetter, podName string) (*CassandraPodSecrets, error) {
	// "cassandra.datastax.com/datacenter" from the pod
	// from that, get "cassdc <labelvalue>"
	// check if managed by helm - if not, an error..

	// helm secret: sh.helm.release.v1.demo.v1
	// after upgrade:
	// sh.helm.release.v1.demo.v2
	// etc

	// func (dc *CassandraDatacenter) GetSuperuserSecretNamespacedName() types.NamespacedName {
	return nil, nil
}
