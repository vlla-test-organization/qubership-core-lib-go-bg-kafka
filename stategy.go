package blue_green_kafka

import (
	"context"
	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"time"
)

var (
	StrategyLatest   = OffsetSetupStrategy{shift: 0, description: "LATEST"}
	StrategyEarliest = OffsetSetupStrategy{shift: 0, description: "EARLIEST"}
)

type OffsetSetupStrategy struct {
	shift       time.Duration
	description string
}

func Rewind(rewindInterval time.Duration) OffsetSetupStrategy {
	return OffsetSetupStrategy{
		shift:       rewindInterval,
		description: "rewind on " + rewindInterval.String(),
	}
}

func (os *OffsetSetupStrategy) getShift() time.Duration {
	return os.shift
}

type OffsetInheritanceStrategy func(ctx context.Context, current GroupId, previous []groupIdWithOffset) (map[TopicPartition]OffsetAndMetadata, error)

func compareGroupStates(group GroupId, current bgMonitor.State, sibling bgMonitor.State) bool {
	if vg, ok := group.(*VersionedGroupId); ok {
		return vg.State == current && vg.SiblingState == sibling
	}
	return false
}
