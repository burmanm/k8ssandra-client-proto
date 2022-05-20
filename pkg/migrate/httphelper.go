package migrate

import (
	"context"

	"github.com/k8ssandra/cass-operator/pkg/httphelper"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// We have no CassandraDatacenter yet, so we need to rewrite parts of the httphelper initialization
func NewManagementClient(ctx context.Context, client client.Client) (httphelper.NodeMgmtClient, error) {
	logger := log.FromContext(ctx)

	// We don't support authentication yet, so always use insecure
	provider := &httphelper.InsecureManagementApiSecurityProvider{}
	protocol := provider.GetProtocol()

	httpClient, err := provider.BuildHttpClient(client, ctx)
	if err != nil {
		logger.Error(err, "error in BuildManagementApiHttpClient")
		return httphelper.NodeMgmtClient{}, err
	}

	return httphelper.NodeMgmtClient{
		Client:   httpClient,
		Log:      logger,
		Protocol: protocol,
	}, nil
}
