package blue_green_kafka

import (
	"context"
	"fmt"
)

type ConsumerConsistencyMode string

const Eventual ConsumerConsistencyMode = "EVENTUAL"
const GuaranteeConsumption ConsumerConsistencyMode = "GUARANTEE_CONSUMPTION"

type offsetManager struct {
	config BgKafkaConsumerConfig
}

func (m *offsetManager) alignOffset(ctx context.Context, groupId GroupId) error {
	consistencyMode := m.config.ConsistencyMode()
	var offsetStrategy OffsetInheritanceStrategy
	switch consistencyMode {
	case Eventual:
		offsetStrategy = EventualStrategy()
	case GuaranteeConsumption:
		offsetStrategy = GuaranteeConsistencyStrategy()
	}
	if offsetStrategy == nil {
		return fmt.Errorf("unsupported consuming consistency mode: %s", consistencyMode)
	}
	logger.InfoC(ctx, "Aligning offset with corrector of consistency mode: %s", consistencyMode)
	admin, err := m.config.AdminSupplier()
	if err != nil {
		return fmt.Errorf("failed to get admin: %w", err)
	}
	indexer, err := newOffsetIndexer(ctx, groupId.GetGroupIdPrefix(), m.config.Topic(), admin)
	if err != nil {
		return fmt.Errorf("failed to create OffsetIndexer: %w", err)
	}
	corrector := &offsetCorrector{
		topic:                        m.config.Topic(),
		indexer:                      indexer,
		inherit:                      offsetStrategy,
		admin:                        admin,
		activeOffsetSetupStrategy:    m.config.ActiveOffsetSetupStrategy(),
		candidateOffsetSetupStrategy: m.config.CandidateOffsetSetupStrategy(),
	}
	return corrector.align(ctx, groupId)
	// todo design proper deletion (without deleting groups in use)
}
