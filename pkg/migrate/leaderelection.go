package migrate

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	LeaderElectionID = "migrator.k8ssandra.io"
)

func NewResourceLock(namespace string) (resourcelock.Interface, error) {
	config, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	// Leader id, needs to be unique
	id, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	padder, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	id = id + "_" + padder.String()

	// Construct clients for leader election
	rest.AddUserAgent(config, "leader-election")
	corev1Client, err := corev1client.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	coordinationClient, err := coordinationv1client.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return resourcelock.New(resourcelock.LeasesResourceLock,
		namespace,
		LeaderElectionID,
		corev1Client,
		coordinationClient,
		resourcelock.ResourceLockConfig{
			Identity: id,
		})
}

func RunLeaderElection(ctx context.Context, wg *sync.WaitGroup, lock resourcelock.Interface) {
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   10 * time.Second,
		RenewDeadline:   5 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				// Indicate we've acquired the lock..
				wg.Done()
			},
			OnStoppedLeading: func() {
				// TODO This is fatal to our usage..
			},
		},
	})
}
