//go:build remote

package logger

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileLoggerCollectsAndPersistsLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.log")
	t.Setenv(remoteLogPathEnv, path)
	log, err := newFileLogger("test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })

	log.Info("captured message")

	require.Contains(t, log.CollectLogs()[0], "captured message")
	require.FileExists(t, path)
}
