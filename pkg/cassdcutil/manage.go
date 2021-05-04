package cassdcutil

import (
	"context"
	"time"

	cassdcapi "github.com/k8ssandra/cass-operator/operator/pkg/apis/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	waitutil "k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type CassManager struct {
	client client.Client
}

func NewManager(client client.Client) *CassManager {
	return &CassManager{
		client: client,
	}
}

// CassandraDatacenter fetches the CassandraDatacenter by its name and namespace
func (c *CassManager) CassandraDatacenter(name, namespace string) (*cassdcapi.CassandraDatacenter, error) {
	cassdcKey := types.NamespacedName{Namespace: namespace, Name: name}
	cassdc := &cassdcapi.CassandraDatacenter{}

	if err := c.client.Get(context.TODO(), cassdcKey, cassdc); err != nil {
		// r.Log.Error(err, "failed to get cassandradatacenter", "CassandraDatacenter", cassdcKey)
		// return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		return nil, err
	}

	return cassdc, nil
}

// CassandraDatacenterPods returns the pods of the CassandraDatacenter
func (c *CassManager) CassandraDatacenterPods(cassdc *cassdcapi.CassandraDatacenter) (*corev1.PodList, error) {
	// What if same namespace has two datacenters with the same name? Can that happen?
	podList := &corev1.PodList{}
	err := c.client.List(context.TODO(), podList, client.InNamespace(cassdc.Namespace), client.MatchingLabels(map[string]string{"cassandra.datastax.com/datacenter": cassdc.Name}))
	return podList, err
}

// ModifyStoppedState either stops or starts the cluster and does nothing if the state is already as requested
func (c *CassManager) ModifyStoppedState(name, namespace string, stop, wait bool) error {
	cassdc, err := c.CassandraDatacenter(name, namespace)
	if err != nil {
		return err
	}

	cassdc = cassdc.DeepCopy()

	cassdc.Spec.Stopped = stop
	if err := c.client.Update(context.TODO(), cassdc); err != nil {
		// r.Log.Error(err, "failed to update the cassandradatacenter", "CassandraDatacenter", cassdcKey)
		// return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		return err
	}

	if wait {
		if stop {
			err = waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
				return c.refreshStatus(cassdc, cassdcapi.DatacenterStopped, corev1.ConditionTrue)
			})
			if err != nil {
				return err
			}

			// And wait for it to finish..
			return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
				return c.refreshStatus(cassdc, cassdcapi.DatacenterReady, corev1.ConditionFalse)
			})
		}

		err = waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			return c.refreshStatus(cassdc, cassdcapi.DatacenterStopped, corev1.ConditionFalse)
		})
		if err != nil {
			return err
		}

		// And wait for it to finish..
		return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			return c.refreshStatus(cassdc, cassdcapi.DatacenterReady, corev1.ConditionTrue)
		})
	}

	return nil
}

// RollingRestart causes the CassandraDatacenter to restart all its pods
func (c *CassManager) RollingRestart(name, namespace string, wait bool) error {
	cassdc, err := c.CassandraDatacenter(name, namespace)
	if err != nil {
		return err
	}

	cassdc = cassdc.DeepCopy()
	cassdc.Spec.RollingRestartRequested = true
	if err := c.client.Update(context.TODO(), cassdc); err != nil {
		// r.Log.Error(err, "failed to update the cassandradatacenter", "CassandraDatacenter", cassdcKey)
		// return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		return err
	}

	if wait {
		// Wait for rolling restart to start..
		err = waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			return c.refreshStatus(cassdc, cassdcapi.DatacenterRollingRestart, corev1.ConditionTrue)
		})
		if err != nil {
			return err
		}

		// And wait for it to finish..
		return waitutil.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			return c.refreshStatus(cassdc, cassdcapi.DatacenterRollingRestart, corev1.ConditionFalse)
		})
	}

	return nil
}

func (c *CassManager) refreshStatus(cassdc *cassdcapi.CassandraDatacenter, status cassdcapi.DatacenterConditionType, wanted corev1.ConditionStatus) (bool, error) {
	cassdc, err := c.CassandraDatacenter(cassdc.Name, cassdc.Namespace)
	if err != nil {
		return false, err
	}

	return cassdc.Status.GetConditionStatus(status) == wanted, nil
}
