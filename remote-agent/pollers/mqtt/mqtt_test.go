package mqtt

import (
	"testing"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/stretchr/testify/require"
)

func TestRequestTrackerResolvesAndDeletes(t *testing.T) {
	tracker := NewRequestTracker(2, time.Minute)
	requestID, responseChannel, err := tracker.Register()
	require.NoError(t, err)
	require.True(t, tracker.Resolve(requestID, v1alpha2.COAResponse{State: v1alpha2.OK}))
	require.Equal(t, v1alpha2.OK, (<-responseChannel).State)

	tracker.Delete(requestID)
	require.False(t, tracker.Resolve(requestID, v1alpha2.COAResponse{}))
}

func TestRequestTrackerBoundsAndExpiresEntries(t *testing.T) {
	now := time.Now()
	tracker := NewRequestTracker(1, time.Minute)
	tracker.now = func() time.Time { return now }
	_, _, err := tracker.Register()
	require.NoError(t, err)
	_, _, err = tracker.Register()
	require.ErrorContains(t, err, "capacity")

	now = now.Add(2 * time.Minute)
	_, _, err = tracker.Register()
	require.NoError(t, err)
}
