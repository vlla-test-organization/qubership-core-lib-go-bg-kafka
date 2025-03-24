package blue_green_kafka

import (
	"context"
	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewDefaultTrackingVersionFilterActive(t *testing.T) {
	assertions := require.New(t)
	statePublisher := &testBGStatePublisher{callbacks: []func(state bgMonitor.BlueGreenState){}}
	filter := NewTrackingVersionFilter(context.Background(), statePublisher)

	statePublisher.setState(bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "test-namespace-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	assertions.True(filter.Test("v1"))
	assertions.Equal("true", filter.String())
}

func TestNewDefaultTrackingVersionFilterActiveAndCandidate(t *testing.T) {
	assertions := require.New(t)

	state := bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "test-namespace-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		Sibling:    &bgMonitor.NamespaceVersion{Namespace: "test-namespace-2", Version: bgMonitor.NewVersionMust("v2"), State: bgMonitor.StateCandidate},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	statePublisher := &testBGStatePublisher{state: state, callbacks: []func(state bgMonitor.BlueGreenState){}}
	filter := NewTrackingVersionFilter(context.Background(), statePublisher)

	assertions.True(filter.Test("v1"))
	assertions.False(filter.Test("v2"))
	assertions.Equal("!v2", filter.String())
}

type testBGStatePublisher struct {
	state     bgMonitor.BlueGreenState
	callbacks []func(state bgMonitor.BlueGreenState)
}

func (t *testBGStatePublisher) Subscribe(ctx context.Context, callback func(state bgMonitor.BlueGreenState)) {
	t.callbacks = append(t.callbacks, callback)
}

func (t *testBGStatePublisher) GetState() bgMonitor.BlueGreenState {
	return t.state
}

func (t *testBGStatePublisher) setState(state bgMonitor.BlueGreenState) {
	t.state = state
	for _, callback := range t.callbacks {
		callback(state)
	}
}
