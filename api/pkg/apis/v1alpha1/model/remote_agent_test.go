package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteAgentCredentialNameIsDeterministicAndNamespaceScoped(t *testing.T) {
	first := RemoteAgentCredentialName("namespace-a", "edge-01")
	require.Equal(t, first, RemoteAgentCredentialName("namespace-a", "edge-01"))
	require.NotEqual(t, first, RemoteAgentCredentialName("namespace-b", "edge-01"))
	require.LessOrEqual(t, len(first), 63)
}

func TestAgentRequestEmitsCanonicalOperationID(t *testing.T) {
	payload, err := json.Marshal(AgentRequest{OperationID: "tracked-id", Provider: "script", Action: "get"})
	require.NoError(t, err)
	require.JSONEq(t, `{"operationId":"tracked-id","provider":"script","action":"get"}`, string(payload))
}

func TestDecodeAgentRequestAcceptsOperationIDAliases(t *testing.T) {
	for _, field := range []string{OperationIDField, LegacyOperationIDField} {
		t.Run(field, func(t *testing.T) {
			request, err := DecodeAgentRequest([]byte(`{"` + field + `":"tracked-id","provider":"script","action":"get"}`))
			require.NoError(t, err)
			require.Equal(t, "tracked-id", request.OperationID)
		})
	}
}

func TestOperationIDValidationRejectsMissingAndConflictingValues(t *testing.T) {
	_, err := DecodeAgentRequest([]byte(`{"provider":"script","action":"get"}`))
	require.ErrorContains(t, err, OperationIDField)

	_, err = DecodeAgentRequest([]byte(`{"operationId":"canonical","operationID":"legacy","provider":"script","action":"get"}`))
	require.ErrorContains(t, err, "conflicting")

	task := map[string]interface{}{OperationIDField: 42.0}
	require.Error(t, SetTaskOperationID(task, "tracked-id"))
}

func TestSetTaskOperationIDEmitsBothAliases(t *testing.T) {
	task := map[string]interface{}{}
	require.NoError(t, SetTaskOperationID(task, "tracked-id"))
	require.Equal(t, "tracked-id", task[OperationIDField])
	require.Equal(t, "tracked-id", task[LegacyOperationIDField])
}

func TestDecodeAsyncResultAcceptsLegacyOperationID(t *testing.T) {
	result, err := DecodeAsyncResult([]byte(`{"operationID":"tracked-id","namespace":"default","body":""}`))
	require.NoError(t, err)
	require.Equal(t, "tracked-id", result.OperationID)
}
