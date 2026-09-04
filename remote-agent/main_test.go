package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeBrokerURL(t *testing.T) {
	brokerURL, serverName, err := normalizeBrokerURL("127.0.0.1", 8883, true)
	require.NoError(t, err)
	require.Equal(t, "tls://127.0.0.1:8883", brokerURL)
	require.Equal(t, "localhost", serverName)
}

func TestTargetNameFromArgs(t *testing.T) {
	require.Equal(t, "edge-01", targetNameFromArgs([]string{"-protocol=mqtt", "-target-name=edge-01"}))
	require.Equal(t, "edge-02", targetNameFromArgs([]string{"-target-name", "edge-02"}))
}
