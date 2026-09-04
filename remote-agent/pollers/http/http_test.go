package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	target "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/remote-agent/agent"
	"github.com/stretchr/testify/require"
)

type testProvider struct{}

func (*testProvider) Init(providers.IProviderConfig) error { return nil }
func (*testProvider) GetValidationRule(context.Context) model.ValidationRule {
	return model.ValidationRule{}
}
func (*testProvider) Get(context.Context, model.DeploymentSpec, []model.ComponentStep) ([]model.ComponentSpec, error) {
	return nil, nil
}
func (*testProvider) Apply(context.Context, model.DeploymentSpec, model.DeploymentStep, bool) (map[string]model.ComponentResultSpec, error) {
	return map[string]model.ComponentResultSpec{"component": {Status: v1alpha2.Updated}}, nil
}

func TestRecoverProcessesAndAcknowledgesTask(t *testing.T) {
	var result model.AsyncResult
	var resultMu sync.Mutex
	resultAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/tasks":
			require.Equal(t, "true", request.URL.Query().Get("getAll"))
			response.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(response).Encode(model.ProviderPagingRequest{
				RequestList: []map[string]interface{}{{
					"operationId": "operation-1",
					"provider":    "script",
					"action":      "apply",
				}},
			})
		case "/results":
			resultAttempts++
			if resultAttempts == 1 {
				http.Error(response, "temporary", http.StatusServiceUnavailable)
				return
			}
			resultMu.Lock()
			defer resultMu.Unlock()
			require.NoError(t, json.NewDecoder(request.Body).Decode(&result))
			response.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	poller := Poller{
		Agent: agent.Agent{Providers: map[string]target.ITargetProvider{
			"script": &testProvider{},
		}},
		Client:       server.Client(),
		RequestURL:   server.URL + "/tasks",
		ResponseURL:  server.URL + "/results",
		Target:       "edge-01",
		Namespace:    "default",
		RetryBackoff: time.Millisecond,
	}

	require.NoError(t, poller.Recover(context.Background()))
	resultMu.Lock()
	defer resultMu.Unlock()
	require.Equal(t, "operation-1", result.OperationID)
	require.Equal(t, "default", result.Namespace)
	require.Empty(t, result.Error)
	require.Equal(t, 2, resultAttempts)
}

func TestHandleRejectsTaskWithoutOperationID(t *testing.T) {
	poller := Poller{}
	err := poller.handle(context.Background(), map[string]interface{}{
		"provider": "script",
		"action":   "apply",
	})
	require.ErrorContains(t, err, "uncorrelated")
	require.ErrorContains(t, err, model.OperationIDField)
}
