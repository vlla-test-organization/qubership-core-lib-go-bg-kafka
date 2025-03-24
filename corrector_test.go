package blue_green_kafka

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_align_groupExists_noBg1(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	indexer := NewMockOffsetsIndexer(controller)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    StrategyLatest,
		candidateOffsetSetupStrategy: StrategyLatest,
	}
	plainGroup := MustParseGroupId("test-plain-group")

	indexer.EXPECT().exists(plainGroup).Return(true)
	indexer.EXPECT().bg1VersionsExist().Return(false)
	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

func Test_align_groupExists_Bg1Migrated(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	indexer := NewMockOffsetsIndexer(controller)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    StrategyLatest,
		candidateOffsetSetupStrategy: StrategyLatest,
	}
	plainGroup := MustParseGroupId("test-plain-group")

	indexer.EXPECT().exists(plainGroup).Return(true)
	indexer.EXPECT().bg1VersionsExist().Return(true)
	indexer.EXPECT().bg1VersionsMigrated().Return(true)

	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}
