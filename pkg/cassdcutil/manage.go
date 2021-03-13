package cassdcutil

import (
	"context"

	cassdcapi "github.com/datastax/cass-operator/operator/pkg/apis/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// import(

// )

// const ()

type CassManager struct {
	client client.Client
}

func NewManager(client client.Client) *CassManager {
	return &CassManager{
		client: client,
	}
}

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

func (c *CassManager) CassandraDatacenterPods(cassdc *cassdcapi.CassandraDatacenter) (*corev1.PodList, error) {
	// What if same namespace has two datacenters with the same name? Can that happen?
	podList := &corev1.PodList{}
	err := c.client.List(context.TODO(), podList, client.InNamespace(cassdc.Namespace), client.MatchingLabels(map[string]string{"cassandra.datastax.com/datacenter": cassdc.Name}))
	return podList, err
}

func (c *CassManager) ModifyStoppedState(name, namespace string, stop, wait bool) error {
	cassdc, err := c.CassandraDatacenter(name, namespace)
	if err != nil {
		return err
	}

	if cassdc.Spec.Stopped == stop {
		// Already in requested state
		return nil
	}

	cassdc = cassdc.DeepCopy()

	cassdc.Spec.Stopped = stop
	// Patch it
	if err := c.client.Update(context.TODO(), cassdc); err != nil {
		// r.Log.Error(err, "failed to update the cassandradatacenter", "CassandraDatacenter", cassdcKey)
		// return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		return err
	}

	// if wait {
	// 	c.CassandraDatacenterPods(cassdc)
	// }

	// Wait for next time if it's ready
	// r.Log.Info("the cassandradatacenter has been updated and will be shutdown", "CassandraDatacenter", cassdcKey)
	// return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

	// if cassdc.Spec.Stopped {

	// 	// TODO Implement --wait using this approach

	// 	// If cass-operator hasn't finished shutting down all the pods, requeue and check later again
	// 	podList := &corev1.PodList{}
	// 	r.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels(map[string]string{"cassandra.datastax.com/datacenter": name}))

	// 	if len(podList.Items) > 0 {
	// 		// Some pods have not been shutdown yet
	// 		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	// 	}
	// } else {
	// }

	return nil
}
