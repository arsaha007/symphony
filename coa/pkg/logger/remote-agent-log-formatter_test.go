package logger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRemoteAgentLog(t *testing.T) {
	entry, ok := parseRemoteAgentLog(`{"level":"warn","msg":"disk pressure","scope":"provider"}`)
	require.True(t, ok)
	require.Equal(t, "warn", entry.Level)
	require.Equal(t, "disk pressure", entry.Message)
	require.Equal(t, "provider", entry.Scope)
}

func TestParseRemoteAgentLogRejectsPlainText(t *testing.T) {
	_, ok := parseRemoteAgentLog("plain text")
	require.False(t, ok)
}
