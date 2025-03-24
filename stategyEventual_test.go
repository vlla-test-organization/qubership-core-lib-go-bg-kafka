package blue_green_kafka

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEventualStrategy_ActiveIdle_InitDomain(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	current := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")
	previous := []groupIdWithOffset{
		{
			groupId: MustParseGroupId("test"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 1}},
		},
	}
	result, err := EventualStrategy()(ctx, current, previous)
	assertions.NoError(err)
	expected := map[TopicPartition]OffsetAndMetadata{{Topic: "test-topic", Partition: 0}: {Offset: 1}}
	assertions.Equal(expected, result)
}

func TestEventualStrategy_Active_Warmup(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	current := MustParseGroupId("test-v1-a_c-2023-07-07_11-30-00")
	previous := []groupIdWithOffset{
		{
			groupId: MustParseGroupId("test"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 1}},
		},
		{
			groupId: MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 12}},
		},
	}
	result, err := EventualStrategy()(ctx, current, previous)
	assertions.NoError(err)
	expected := map[TopicPartition]OffsetAndMetadata{
		{Topic: "test-topic", Partition: 0}: {Offset: 12}}
	assertions.Equal(expected, result)
}

func TestEventualStrategy_ActiveIdle_Commit(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	current := MustParseGroupId("test-v1-a_i-2023-07-07_12-30-00")
	previous := []groupIdWithOffset{
		{
			groupId: MustParseGroupId("test-v1-a_c-2023-07-07_11-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 23},
				{Topic: "test-topic", Partition: 1}: {Offset: 24}},
		},
		{
			groupId: MustParseGroupId("test-v2-c_a-2023-07-07_11-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 24},
				{Topic: "test-topic", Partition: 1}: {Offset: 20}},
		},
	}
	result, err := EventualStrategy()(ctx, current, previous)
	assertions.NoError(err)
	expected := map[TopicPartition]OffsetAndMetadata{
		{Topic: "test-topic", Partition: 0}: {Offset: 23},
		{Topic: "test-topic", Partition: 1}: {Offset: 24}}
	assertions.Equal(expected, result)
}

func TestEventualStrategy_Active_Promote(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	current := MustParseGroupId("test-v3-a_l-2023-07-07_14-30-00")
	previous := []groupIdWithOffset{
		{
			groupId: MustParseGroupId("test-v1-a_c-2023-07-07_13-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 45},
				{Topic: "test-topic", Partition: 1}: {Offset: 46}},
		},
		{
			groupId: MustParseGroupId("test-v3-c_a-2023-07-07_13-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 46},
				{Topic: "test-topic", Partition: 1}: {Offset: 40}},
		},
	}
	result, err := EventualStrategy()(ctx, current, previous)
	assertions.NoError(err)
	expected := map[TopicPartition]OffsetAndMetadata{
		{Topic: "test-topic", Partition: 0}: {Offset: 45},
		{Topic: "test-topic", Partition: 1}: {Offset: 46}}
	assertions.Equal(expected, result)
}

func TestEventualStrategy_Active_Rollback(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	current := MustParseGroupId("test-v1-a_c-2023-07-07_15-30-00")
	previous := []groupIdWithOffset{
		{
			groupId: MustParseGroupId("test-v3-a_l-2023-07-07_14-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 57},
				{Topic: "test-topic", Partition: 1}: {Offset: 58}},
		},
		{
			groupId: MustParseGroupId("test-v1-l_a-2023-07-07_14-30-00"),
			offset: map[TopicPartition]OffsetAndMetadata{
				{Topic: "test-topic", Partition: 0}: {Offset: 56},
				{Topic: "test-topic", Partition: 1}: {Offset: 50}},
		},
	}
	result, err := EventualStrategy()(ctx, current, previous)
	assertions.NoError(err)
	expected := map[TopicPartition]OffsetAndMetadata{
		{Topic: "test-topic", Partition: 0}: {Offset: 57},
		{Topic: "test-topic", Partition: 1}: {Offset: 58}}
	assertions.Equal(expected, result)
}
