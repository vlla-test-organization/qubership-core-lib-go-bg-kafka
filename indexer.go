package blue_green_kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

//go:generate mockgen -source=indexer.go -destination=indexer_mock.go -package=blue_green_kafka -mock_names=offsetsIndexer=MockOffsetsIndexer

type offsetsIndexer interface {
	exists(search GroupId) bool
	bg1VersionsExist() bool
	bg1VersionsMigrated() bool
	findPreviousStateOffset(ctx context.Context, current GroupId) []groupIdWithOffset
	createMigrationDoneFromBg1MarkerGroup(ctx context.Context) error
}

type offsetsIndexerImpl struct {
	topic string
	index map[GroupId]map[TopicPartition]OffsetAndMetadata
	admin NativeAdminAdapter
}

type TopicPartition struct {
	Partition int
	Topic     string
}

type OffsetAndMetadata struct {
	Offset int64
}

type ConsumerGroup struct {
	GroupId string
}

func newOffsetIndexer(ctx context.Context, groupIdPrefix string, topic string, adminAdapter NativeAdminAdapter) (*offsetsIndexerImpl, error) {
	index := make(map[GroupId]map[TopicPartition]OffsetAndMetadata)
	groups, err := adminAdapter.ListConsumerGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	for _, cg := range groups {
		if !strings.HasPrefix(cg.GroupId, groupIdPrefix) {
			// ignore unrelated groups
			continue
		}
		groupId, err := ParseGroupId(cg.GroupId)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GroupId: %w", err)
		}
		if groupIdPrefix != groupId.GetGroupIdPrefix() {
			continue
		}
		offsets, err := adminAdapter.ListConsumerGroupOffsets(ctx, cg.GroupId)
		if err != nil {
			return nil, fmt.Errorf("failed to list consumer group offsets: %w", err)
		}
		logger.InfoC(ctx, "Offsets=%+v", offsets)
		logger.InfoC(ctx, "Adding to index groupId=%s with offset: %+v", groupId.String(), offsets)
		index[groupId] = offsets
	}
	return &offsetsIndexerImpl{topic: topic, index: index, admin: adminAdapter}, nil
}

func (indexer *offsetsIndexerImpl) exists(search GroupId) bool {
	_, ok := indexer.index[search]
	return ok
}

func (indexer *offsetsIndexerImpl) bg1VersionsExist() bool {
	for g := range indexer.index {
		_, ok := g.(*BG1VersionedGroupId)
		if ok {
			return true
		}
	}
	return false
}

func (indexer *offsetsIndexerImpl) bg1VersionsMigrated() bool {
	for g := range indexer.index {
		bg1g, ok := g.(*BG1VersionedGroupId)
		if ok && bg1g.Stage == bg1StageMigrated {
			return true
		}
	}
	return false
}

func (indexer *offsetsIndexerImpl) createMigrationDoneFromBg1MarkerGroup(ctx context.Context) error {
	filtered := map[GroupId]map[TopicPartition]OffsetAndMetadata{}
	for g, tpOffset := range indexer.index {
		if vg, ok := g.(*BG1VersionedGroupId); ok && vg.Stage == bg1StageActive {
			filtered[vg] = tpOffset
		}
	}
	latestGroupOffset := getLatestGroupOffset(filtered)
	if latestGroupOffset != nil {
		bg1vg := latestGroupOffset.groupId.(*BG1VersionedGroupId)
		migratedMarkerGroup := &BG1VersionedGroupId{
			GroupIdPrefix:    bg1vg.GetGroupIdPrefix(),
			Version:          bg1vg.Version,
			BlueGreenVersion: bg1vg.BlueGreenVersion,
			Stage:            bg1StageMigrated,
			Updated:          time.Now(),
		}
		logger.InfoC(ctx, "Creating migrated from BG1 marker Group: %s", migratedMarkerGroup.String())
		return indexer.admin.AlterConsumerGroupOffsets(ctx, migratedMarkerGroup, indexer.index[bg1vg])
	} else {
		logger.WarnC(ctx, "Did not create 'migrated from BG1 marker Group' because no bg1 active group was found")
		return nil
	}
}

func (indexer *offsetsIndexerImpl) findPreviousStateOffset(ctx context.Context, current GroupId) []groupIdWithOffset {
	logger.InfoC(ctx, "Search the ancestor for: %s", current)
	filtered := map[GroupId]map[TopicPartition]OffsetAndMetadata{}

	for gId, tpOffset := range indexer.index {
		// filter out current group
		if gId == current {
			continue
		}
		// filter out offsets of the same update time (already created in sibling ns)
		if vg, ok1 := gId.(*VersionedGroupId); ok1 {
			if vCurrent, ok2 := current.(*VersionedGroupId); ok2 &&
				(vg.Updated.Equal(vCurrent.Updated) || vg.Updated.After(vCurrent.Updated)) {
				continue
			}
		}
		filtered[gId] = tpOffset
	}
	// find latest groupId
	latestGroupOffset := getLatestGroupOffset(filtered)
	var latestGroups []GroupId
	if latestGroupOffset != nil {
		// if latest groupId is versioned - find all groupIds of the same updateTime
		if vg1, ok1 := latestGroupOffset.groupId.(*VersionedGroupId); ok1 {
			for g2 := range indexer.index {
				if vg2, ok2 := g2.(*VersionedGroupId); ok2 && vg2.Updated.Equal(vg1.Updated) {
					latestGroups = append(latestGroups, g2)
				}
			}
		} else if vg1, ok1 := latestGroupOffset.groupId.(*BG1VersionedGroupId); ok1 {
			// old blue-green groupId, we are interested only in active groupId
			for g2 := range indexer.index {
				if vg2, ok2 := g2.(*BG1VersionedGroupId); ok2 && vg2.Updated.Equal(vg1.Updated) && vg2.Stage == bg1StageActive {
					latestGroups = append(latestGroups, g2)
				}
			}
		} else {
			latestGroups = append(latestGroups, latestGroupOffset.groupId)
		}
	}
	var result []groupIdWithOffset
	for _, g := range latestGroups {
		result = append(result, groupIdWithOffset{
			groupId: g,
			offset:  indexer.index[g],
		})
	}
	if len(result) > 0 {
		var previousStrs []string
		for _, g := range result {
			previousStrs = append(previousStrs, g.String())
		}
		logger.InfoC(ctx, "Ancestors for: '%s' are: %+v", current, previousStrs)
	} else {
		logger.InfoC(ctx, "There are no ancestors for: '%s'", current)
	}
	return result
}

func getLatestGroupOffset(index map[GroupId]map[TopicPartition]OffsetAndMetadata) *groupIdWithOffset {
	result := getSortedIndexKeys(index)
	length := len(result)
	if length > 0 {
		return &result[length-1]
	} else {
		return nil
	}
}

func getSortedIndexKeys(index map[GroupId]map[TopicPartition]OffsetAndMetadata) []groupIdWithOffset {
	keys := make([]GroupId, 0, len(index))
	for g := range index {
		keys = append(keys, g)
	}
	sort.Slice(keys, func(gi1, gi2 int) bool {
		return getSortKeyFromGroup(keys[gi1]).Before(getSortKeyFromGroup(keys[gi2]))
	})
	result := make([]groupIdWithOffset, 0, len(index))
	for _, g := range keys {
		result = append(result, groupIdWithOffset{
			groupId: g,
			offset:  index[g],
		})
	}
	return result
}

var minTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func getSortKeyFromGroup(g GroupId) time.Time {
	if vg, ok := g.(*VersionedGroupId); ok {
		return vg.Updated
	} else if vg, ok := g.(*BG1VersionedGroupId); ok {
		return vg.Updated
	} else {
		return minTime
	}
}
