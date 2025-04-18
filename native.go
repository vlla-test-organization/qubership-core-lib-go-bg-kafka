package blue_green_kafka

//go:generate mockgen -source=native.go -destination=native_mock.go -package=blue_green_kafka -mock_names=Consumer=MockConsumer,NativeKafkaConsumer=MockNativeKafkaConsumer,NativeAdminAdapter=MockNativeAdminAdapter

import (
	"context"
	"time"
)

type Message interface {
	Topic() string
	Offset() int64
	HighWaterMark() int64
	Headers() []Header
	Key() []byte
	Value() []byte
	Partition() int
	NativeMsg() any
}

type Header struct {
	Key   string
	Value []byte
}

type Result struct {
	Message *Message
	Offset  int64
}

type Consumer interface {
	ReadMessage(context.Context) (Message, error)
	Commit(ctx context.Context, marker *CommitMarker) error
	Close() error
}

type NativeAdminAdapter interface {
	ListConsumerGroups(ctx context.Context) ([]ConsumerGroup, error)
	ListConsumerGroupOffsets(ctx context.Context, groupId string) (map[TopicPartition]OffsetAndMetadata, error)
	AlterConsumerGroupOffsets(ctx context.Context, groupIdPrefix GroupId, proposedOffsets map[TopicPartition]OffsetAndMetadata) error
	PartitionsFor(ctx context.Context, topics ...string) ([]PartitionInfo, error)
	BeginningOffsets(ctx context.Context, topicPartitions []TopicPartition) (map[TopicPartition]int64, error)
	EndOffsets(ctx context.Context, topicPartitions []TopicPartition) (map[TopicPartition]int64, error)
	OffsetsForTimes(ctx context.Context, times map[TopicPartition]time.Time) (map[TopicPartition]*OffsetAndTimestamp, error)
}
