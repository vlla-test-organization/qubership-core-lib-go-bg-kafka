package blue_green_kafka

import (
	"context"
	"fmt"
	bgMonitor "github.com/vlla-test-organization/qubership-core-lib-go-bg-state-monitor/v2"
	"sync/atomic"
)

type TrackingVersionFilter struct {
	statePublisher BGStatePublisher
	predicate      *atomic.Pointer[Filter]
}

type BGStatePublisher interface {
	Subscribe(ctx context.Context, callback func(state bgMonitor.BlueGreenState))
	GetState() bgMonitor.BlueGreenState
}

func NewTrackingVersionFilter(ctx context.Context, statePublisher BGStatePublisher) *TrackingVersionFilter {
	instance := TrackingVersionFilter{statePublisher: statePublisher, predicate: &atomic.Pointer[Filter]{}}
	instance.predicate.Store(NewFilter(statePublisher.GetState()))
	statePublisher.Subscribe(ctx, func(state bgMonitor.BlueGreenState) {
		instance.predicate.Store(NewFilter(state))
	})
	return &instance
}

func (f *TrackingVersionFilter) Test(version string) (bool, error) {
	return f.predicate.Load().Test(version)
}

func (f *TrackingVersionFilter) String() string {
	return f.predicate.Load().Presentation
}

func NewFilter(bgState bgMonitor.BlueGreenState) *Filter {
	currentNsVersion := bgState.Current
	siblingNsV := bgState.Sibling
	blueGreenState := currentNsVersion.State
	if siblingNsV == nil || siblingNsV.State == bgMonitor.StateIdle {
		return newFilter("true", func(v string) (bool, error) { return true, nil })
	} else {
		switch blueGreenState {
		case bgMonitor.StateActive:
			siblingVersion := siblingNsV.Version
			return newFilter("!"+siblingVersion.String(), func(v string) (bool, error) {
				version, err := bgMonitor.NewVersion(v)
				if err != nil {
					return false, err
				}
				return *version != *siblingVersion, nil
			})
		case bgMonitor.StateCandidate, bgMonitor.StateLegacy:
			currentVersion := currentNsVersion.Version
			return newFilter(currentVersion.String(), func(v string) (bool, error) {
				version, err := bgMonitor.NewVersion(v)
				if err != nil {
					return false, err
				}
				return *version == *currentVersion, nil
			})
		default:
			return newFilter("false", func(v string) (bool, error) {
				return false, fmt.Errorf("invalid state: " + blueGreenState.String())
			})
		}
	}
}

type Filter struct {
	Presentation string
	Test         func(version string) (bool, error)
}

func newFilter(presentation string, checkFunc func(version string) (bool, error)) *Filter {
	return &Filter{Presentation: presentation, Test: checkFunc}
}
