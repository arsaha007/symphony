package remoteagent

import (
	"testing"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/stretchr/testify/require"
)

func TestDesiredRemoteAgentComponentConverges(t *testing.T) {
	component := model.ComponentSpec{Name: "remote-agent", Type: "remote-agent", Properties: map[string]interface{}{"description": "edge"}}

	upgrade, changed := desiredRemoteAgentComponent(component, map[string]string{"version": "1", "os": "linux"}, "2", "thumb", "checksum")
	require.True(t, changed)
	require.Equal(t, "upgrade", upgrade.Properties["action"])
	require.Equal(t, "2", upgrade.Properties["version"])
	require.Equal(t, "checksum", upgrade.Properties["sha256"])

	rotation, changed := desiredRemoteAgentComponent(upgrade, map[string]string{"version": "2", "certificateThumbprint": "old"}, "2", "thumb", "")
	require.True(t, changed)
	require.Equal(t, "secretrotation", rotation.Properties["action"])
	require.Equal(t, "thumb", rotation.Properties["thumbprint"])

	converged, changed := desiredRemoteAgentComponent(rotation, map[string]string{"version": "2", "certificateThumbprint": "thumb"}, "2", "thumb", "")
	require.True(t, changed)
	require.NotContains(t, converged.Properties, "action")
	require.Equal(t, "edge", converged.Properties["description"])

	_, changed = desiredRemoteAgentComponent(converged, map[string]string{"version": "2", "certificateThumbprint": "thumb"}, "2", "thumb", "")
	require.False(t, changed)
}

func TestDesiredRemoteAgentComponentSkipsWindowsUpgrade(t *testing.T) {
	component := model.ComponentSpec{Name: "remote-agent", Type: "remote-agent"}
	_, changed := desiredRemoteAgentComponent(component, map[string]string{"version": "1", "os": "windows"}, "2", "", "")
	require.False(t, changed)
}

func TestRemoteAgentStatusReadsTargetStatuses(t *testing.T) {
	target := model.TargetState{ObjectMeta: model.ObjectMeta{Name: "edge-01"}}
	target.Status.SetComponentStatus("edge-01", "remote-agent", `Updated - {"version":"2","certificateThumbprint":"abc"}`)
	status, ok := remoteAgentStatus(target, "remote-agent")
	require.True(t, ok)
	require.Equal(t, "2", status["version"])
}
