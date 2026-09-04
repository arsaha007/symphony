package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	target "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/target"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/stretchr/testify/require"
)

type testProvider struct {
	getErr   error
	applyErr error
}

func (*testProvider) Init(providers.IProviderConfig) error { return nil }

func (*testProvider) GetValidationRule(context.Context) model.ValidationRule {
	return model.ValidationRule{AllowSidecar: true}
}

func (p *testProvider) Get(context.Context, model.DeploymentSpec, []model.ComponentStep) ([]model.ComponentSpec, error) {
	return []model.ComponentSpec{{Name: "current"}}, p.getErr
}

func (p *testProvider) Apply(context.Context, model.DeploymentSpec, model.DeploymentStep, bool) (map[string]model.ComponentResultSpec, error) {
	return map[string]model.ComponentResultSpec{
		"component": {Status: v1alpha2.Updated},
	}, p.applyErr
}

func TestHandleGet(t *testing.T) {
	dispatcher := Agent{Providers: map[string]target.ITargetProvider{"role": &testProvider{}}}
	request := model.ProviderGetRequest{
		AgentRequest: model.AgentRequest{OperationID: "op-1", Provider: "role", Action: "get"},
	}
	payload, err := json.Marshal(request)
	require.NoError(t, err)

	result := dispatcher.Handle(payload, context.Background())

	require.Empty(t, result.Error)
	var specs []model.ComponentSpec
	require.NoError(t, json.Unmarshal(result.Body, &specs))
	require.Equal(t, "current", specs[0].Name)
}

func TestHandlePreservesProviderError(t *testing.T) {
	dispatcher := Agent{Providers: map[string]target.ITargetProvider{
		"role": &testProvider{applyErr: errors.New("apply failed")},
	}}
	request := model.ProviderApplyRequest{
		AgentRequest: model.AgentRequest{OperationID: "op-2", Provider: "role", Action: "apply"},
	}
	payload, err := json.Marshal(request)
	require.NoError(t, err)

	result := dispatcher.Handle(payload, context.Background())

	require.Equal(t, "apply failed", result.Error)
	require.NotEmpty(t, result.Body)
}

func TestHandleRejectsInvalidRequests(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		providers  map[string]target.ITargetProvider
		errorMatch string
	}{
		{name: "malformed", payload: "{", errorMatch: "unexpected end"},
		{name: "missing operation ID", payload: `{"provider":"role","action":"get"}`, providers: map[string]target.ITargetProvider{"role": &testProvider{}}, errorMatch: model.OperationIDField},
		{name: "unknown provider", payload: `{"operationID":"op","provider":"missing","action":"get"}`, errorMatch: "not found"},
		{name: "unknown action", payload: `{"operationID":"op","provider":"role","action":"remove"}`, providers: map[string]target.ITargetProvider{"role": &testProvider{}}, errorMatch: "action"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dispatcher := Agent{Providers: test.providers}
			result := dispatcher.Handle([]byte(test.payload), context.Background())
			require.Contains(t, result.Error, test.errorMatch)
		})
	}
}

func TestExecutionCacheDeduplicatesConcurrentOperations(t *testing.T) {
	cache := NewExecutionCache(10, time.Minute)
	var calls atomic.Int32
	execute := func() model.AsyncResult {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return model.AsyncResult{OperationID: "operation"}
	}
	results := make(chan model.AsyncResult, 2)
	go func() { results <- cache.Do(context.Background(), "operation", execute) }()
	go func() { results <- cache.Do(context.Background(), "operation", execute) }()
	<-results
	<-results
	require.Equal(t, int32(1), calls.Load())
}

func TestExecutionCacheRejectsNewWorkAtCapacity(t *testing.T) {
	cache := NewExecutionCache(1, time.Minute)
	started := make(chan struct{})
	release := make(chan struct{})
	go cache.Do(context.Background(), "first", func() model.AsyncResult {
		close(started)
		<-release
		return model.AsyncResult{OperationID: "first"}
	})
	<-started
	result := cache.Do(context.Background(), "second", func() model.AsyncResult {
		return model.AsyncResult{OperationID: "second"}
	})
	close(release)
	require.Contains(t, result.Error, "capacity")
}

func TestExecutionCacheEvictsCompletedWorkAtCapacity(t *testing.T) {
	cache := NewExecutionCache(1, time.Minute)
	first := cache.Do(context.Background(), "first", func() model.AsyncResult {
		return model.AsyncResult{OperationID: "first"}
	})
	require.Empty(t, first.Error)
	second := cache.Do(context.Background(), "second", func() model.AsyncResult {
		return model.AsyncResult{OperationID: "second"}
	})
	require.Empty(t, second.Error)
	require.Equal(t, "second", second.OperationID)
}
