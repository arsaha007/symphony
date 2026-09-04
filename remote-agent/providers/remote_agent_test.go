package providers

import (
	"encoding/base64"
	"encoding/pem"
	"testing"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/stretchr/testify/require"
)

func TestNormalizePEMReconstructsSpaceSeparatedCertificate(t *testing.T) {
	der := []byte("certificate bytes")
	encoded := base64.StdEncoding.EncodeToString(der)
	data, err := normalizePEM("-----BEGIN CERTIFICATE----- "+encoded+" -----END CERTIFICATE-----", "CERTIFICATE")
	require.NoError(t, err)
	block, _ := pem.Decode(data)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)
	require.Equal(t, der, block.Bytes)
}

func TestUpgradeRequiresVersion(t *testing.T) {
	provider := &RemoteAgentProvider{}
	err := provider.upgrade(t.Context(), structComponent(nil))
	require.ErrorContains(t, err, "version")
}

func structComponent(properties map[string]interface{}) model.ComponentSpec {
	return model.ComponentSpec{Properties: properties}
}
