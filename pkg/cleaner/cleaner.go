package cleaner

import (
	"context"
	"log"
	"time"

	cassdcapi "github.com/datastax/cass-operator/operator/pkg/apis/cassandra/v1beta1"
	medusa "github.com/k8ssandra/medusa-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	waitutil "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	managedLabel      = "app.kubernetes.io/managed-by"
	managedLabelValue = "Helm"
	instanceLabel     = "app.kubernetes.io/instance"
	nameLabel         = "app.kubernetes.io/name"
	nameLabelValue    = "k8ssandra"
	releaseAnnotation = "meta.helm.sh/release-name"
)

// Agent is a cleaner utility for resources which helm pre-delete requires
type Agent struct {
	Client    client.Client
	Namespace string
}

// New returns a new instance of cleaning agent
func New(namespace string) (*Agent, error) {
	_ = api.AddToScheme(scheme.Scheme)
	_ = cassdcapi.AddToScheme(scheme.Scheme)

	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme.Scheme})
	if err != nil {
		log.Fatal(err)
	}

	// Ensure all operations are targeting a single namespace
	c = client.NewNamespacedClient(c, namespace)

	return &Agent{
		Client:    c,
		Namespace: namespace,
	}, nil
}

func (a *Agent) RemoveCassandraBackups(releaseName string, wait bool) error {
	// Should be related to a removed CassandraDatacenter.. spec.cassandraDatacenter has it
	list := &medusa.CassandraBackupList{}
	err := a.Client.List(context.Background(), list, client.InNamespace(a.Namespace), client.MatchingLabels(map[string]string{instanceLabel: releaseName}))
	if err != nil {
		log.Fatalf("Failed to list CassandraBackups in namespace %s for release %s", a.Namespace, releaseName)
		return err
	}

	for _, backup := range list.Items {
		err = a.Client.Delete(context.Background(), &backup)
		if err != nil {
			log.Fatalf("Failed to delete CassandraBackup: %v\n", backup)
			return err
		}
	}

	if wait {
		return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			list := &medusa.CassandraBackupList{}
			err := a.Client.List(context.Background(), list, client.InNamespace(a.Namespace), client.MatchingLabels(map[string]string{instanceLabel: releaseName}))
			if err != nil {
				log.Printf("failed to list CassandraBackups: %s\n", err)
				return false, err
			}
			return len(list.Items) == 0, nil
		})
	}

	return nil
}

func (a *Agent) RemoveCassandraDatacenters(releaseName string, wait bool) error {
	log.Printf("Removing CassandraDatacenter(s) managed in release %s from namespace %s\n", releaseName, a.Namespace)
	releaseLabels := client.MatchingLabels{
		managedLabel:  managedLabelValue,
		instanceLabel: releaseName,
		nameLabel:     nameLabelValue,
	}
	list := &cassdcapi.CassandraDatacenterList{}
	err := a.Client.List(context.Background(), list, client.InNamespace(a.Namespace), releaseLabels)
	if err != nil {
		log.Fatalf("Failed to list CassandraDatacenters in namespace: %s", a.Namespace)
		return err
	}

	for _, cassdc := range list.Items {
		if err = a.Client.Delete(context.Background(), &cassdc); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsResourceExpired(err) {
			log.Fatalf("failed to delete CassandraDatacenter %s: %s",
				types.NamespacedName{Namespace: cassdc.Namespace, Name: cassdc.Name}, err)
		}
	}

	// We need to wait until the CassandraDatacenter is terminated; otherwise, cass-operator could get
	// deleted before it has a chance to clear the CassandraDatacenter's finalizer.
	if wait {
		return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			list := &cassdcapi.CassandraDatacenterList{}
			err := a.Client.List(context.Background(), list, client.InNamespace(a.Namespace), releaseLabels)
			if err != nil {
				log.Printf("failed to list CassandraDatacenters: %s\n", err)
				return false, err
			}
			return len(list.Items) == 0, nil
		})
	}

	return nil
}
