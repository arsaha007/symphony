package solutionversion

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states/memorystate"
	loggercontexts "github.com/eclipse-symphony/symphony/coa/pkg/logger/contexts"
	"github.com/stretchr/testify/require"
)

type remoteOperationTestProvider struct{}

func (*remoteOperationTestProvider) Init(providers.IProviderConfig) error { return nil }
func (*remoteOperationTestProvider) GetValidationRule(context.Context) model.ValidationRule {
	return model.ValidationRule{}
}
func (*remoteOperationTestProvider) Get(context.Context, model.DeploymentSpec, []model.ComponentStep) ([]model.ComponentSpec, error) {
	return nil, nil
}
func (*remoteOperationTestProvider) Apply(context.Context, model.DeploymentSpec, model.DeploymentStep, bool) (map[string]model.ComponentResultSpec, error) {
	return map[string]model.ComponentResultSpec{"component": {Status: v1alpha2.Updated}}, nil
}

func TestRemoteOperationLeaseAndCompletion(t *testing.T) {
	state := &memorystate.MemoryStateProvider{}
	require.NoError(t, state.Init(memorystate.MemoryStateProviderConfig{}))
	manager := &SolutionVersionManager{SummaryManager: SummaryManager{StateProvider: state}}
	request := model.AgentRequest{OperationID: "operation-1", Provider: "script", Action: "get"}
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	require.NoError(t, manager.ensureRemoteOperation(t.Context(), model.RemoteOperation{
		OperationID: request.OperationID,
		Target:      "edge-01",
		Namespace:   "default",
		Request:     payload,
		Status:      model.RemoteOperationQueued,
	}))

	page, err := manager.GetRemoteTasks(t.Context(), "edge-01", "default", false, "", 1)
	require.NoError(t, err)
	require.Len(t, page.RequestList, 1)
	require.Equal(t, request.OperationID, page.RequestList[0][model.OperationIDField])
	require.Equal(t, request.OperationID, page.RequestList[0][model.LegacyOperationIDField])
	correlationKey := loggercontexts.ConstructHttpHeaderKeyForActivityLogContext(loggercontexts.Activity_CorrelationId)
	require.Equal(t, request.OperationID, page.RequestList[0][correlationKey])
	secondPage, err := manager.GetRemoteTasks(t.Context(), "edge-01", "default", false, "", 1)
	require.NoError(t, err)
	require.Empty(t, secondPage.RequestList)
	recoveryPage, err := manager.GetRemoteTasks(t.Context(), "edge-01", "default", true, "0", 10)
	require.NoError(t, err)
	require.Empty(t, recoveryPage.RequestList)

	asyncResult := model.AsyncResult{
		OperationID: request.OperationID,
		Namespace:   "default",
		Body:        []byte(`[]`),
	}
	require.Error(t, manager.HandleRemoteAgentExecuteResultForTarget(t.Context(), asyncResult, "edge-02"))
	require.NoError(t, manager.HandleRemoteAgentExecuteResultForTarget(t.Context(), asyncResult, "edge-01"))
	result, err := manager.waitForRemoteOperation(context.Background(), request.OperationID, "default")
	require.NoError(t, err)
	require.Equal(t, request.OperationID, result.OperationID)
}

func TestRemoteOperationIDIsDeterministicAndActionSpecific(t *testing.T) {
	deployment := model.DeploymentSpec{Generation: "1", Hash: "hash", JobID: "7"}
	deployment.Instance.ObjectMeta.Name = "instance"
	step := model.DeploymentStep{Target: "edge-01", Role: "script"}
	require.Equal(t, remoteOperationID(deployment, "get", step), remoteOperationID(deployment, "get", step))
	require.NotEqual(t, remoteOperationID(deployment, "get", step), remoteOperationID(deployment, "apply", step))
}

func TestExecuteRemoteApplyEndToEnd(t *testing.T) {
	state := &memorystate.MemoryStateProvider{}
	require.NoError(t, state.Init(memorystate.MemoryStateProviderConfig{}))
	manager := &SolutionVersionManager{
		SummaryManager: SummaryManager{
			Manager:       managers.Manager{Config: managers.ManagerConfig{Properties: map[string]string{"remoteAgent.operationTimeout": "5s"}}},
			StateProvider: state,
		},
	}
	step := model.DeploymentStep{
		Target: "edge-01",
		Role:   "script",
		Components: []model.ComponentStep{{
			Action:    model.ComponentUpdate,
			Component: model.ComponentSpec{Name: "component", Type: "script"},
		}},
	}
	deployment := model.DeploymentSpec{Generation: "1", Hash: "hash", JobID: "1"}
	deployment.Instance.ObjectMeta.Name = "instance"
	done := make(chan map[string]model.ComponentResultSpec, 1)
	errors := make(chan error, 1)
	go func() {
		result, err := manager.executeRemoteApply(context.Background(), deployment, step, "default")
		if err != nil {
			errors <- err
			return
		}
		done <- result
	}()

	var page model.ProviderPagingRequest
	require.Eventually(t, func() bool {
		var err error
		page, err = manager.GetRemoteTasks(t.Context(), "edge-01", "default", false, "", 1)
		return err == nil && len(page.RequestList) == 1
	}, 2*time.Second, 20*time.Millisecond)
	payload, err := json.Marshal(page.RequestList[0])
	require.NoError(t, err)
	var request model.ProviderApplyRequest
	require.NoError(t, json.Unmarshal(payload, &request))
	componentResults, err := (&remoteOperationTestProvider{}).Apply(t.Context(), request.Deployment, request.Step, request.IsDryRun)
	require.NoError(t, err)
	body, err := json.Marshal(componentResults)
	require.NoError(t, err)
	result := model.AsyncResult{OperationID: request.OperationID, Namespace: "default", Body: body}
	require.NoError(t, manager.HandleRemoteAgentExecuteResult(t.Context(), result))

	select {
	case err := <-errors:
		require.NoError(t, err)
	case applied := <-done:
		require.Equal(t, v1alpha2.Updated, applied["component"].Status)
	case <-time.After(3 * time.Second):
		t.Fatal("remote apply did not complete")
	}
}
