package blue_green_kafka

import (
	"sync"
)

type PartitionedInt64Metric struct {
	values map[int]int64
}

func (pm *PartitionedInt64Metric) set(partition int, value int64) {
	pm.values[partition] = value
}

func (pm *PartitionedInt64Metric) inc(partition int) {
	pm.values[partition]++
}

func (pm *PartitionedInt64Metric) drop() {
	pm.values = make(map[int]int64, len(pm.values))
}

func (pm *PartitionedInt64Metric) copy() *PartitionedInt64Metric {
	c := make(map[int]int64, len(pm.values))
	for k, v := range pm.values {
		c[k] = v
	}
	return &PartitionedInt64Metric{values: c}
}

func (pm *PartitionedInt64Metric) GetByPartitions() map[int]int64 {
	return pm.values
}

func newPartitionedInt64Metric() *PartitionedInt64Metric {
	return &PartitionedInt64Metric{
		values: make(map[int]int64),
	}
}

// Metrics contains BG consumer metrics update on last message poll
// Values split by partitions and contains data only from partitions assigned to this consumer
type Metrics struct {
	// Topic is a name of consuming topic
	Topic string

	// HighWatermark is the latest offset available on the broker for this partition
	HighWatermark *PartitionedInt64Metric
	// LastPollOffset the most recently read message offset on the partition
	LastPollOffset *PartitionedInt64Metric
	// CommitOffset is the commited message offset
	CommitOffset *PartitionedInt64Metric
	// Lag is a difference between HighWatermark-1 and CommitOffset - number of unconsumed messages
	Lag *PartitionedInt64Metric

	// ConsumedMessageCount is a number of messages consumed for this partition (succeed message polls)
	ConsumedMessageCount *PartitionedInt64Metric
	// AcceptedMessageCount is a number of messages accepted by blue-green state filter
	AcceptedMessageCount *PartitionedInt64Metric
	// FilteredMessageCount is a number of messages filtered by blue-green state filter
	FilteredMessageCount *PartitionedInt64Metric
	// CommitsCount is a number of succeed offset commits
	CommitCount *PartitionedInt64Metric

	mu sync.RWMutex
}

type pollMetricsUpdate struct {
	partition     int
	highWaterMark int64
	currentOffset int64
	isAccepted    bool
}

type commitMetricsUpdate struct {
	partition    int
	commitOffset int64
}

func NewConsumerMetrics(topic string) *Metrics {
	return &Metrics{
		Topic:                topic,
		HighWatermark:        newPartitionedInt64Metric(),
		LastPollOffset:       newPartitionedInt64Metric(),
		CommitOffset:         newPartitionedInt64Metric(),
		Lag:                  newPartitionedInt64Metric(),
		ConsumedMessageCount: newPartitionedInt64Metric(),
		AcceptedMessageCount: newPartitionedInt64Metric(),
		FilteredMessageCount: newPartitionedInt64Metric(),
		CommitCount:          newPartitionedInt64Metric(),
		mu:                   sync.RWMutex{},
	}
}

func (m *Metrics) updateMetricsOnPoll(update *pollMetricsUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	partition := update.partition

	m.ConsumedMessageCount.inc(partition)
	if update.isAccepted {
		m.AcceptedMessageCount.inc(partition)
	} else {
		m.FilteredMessageCount.inc(partition)
	}

	lastHighWaterMark, exists := m.HighWatermark.values[partition]
	if exists && lastHighWaterMark > update.highWaterMark {
		// skip message with lower high watermark as it can be old message processed with invalid order
		return
	}
	m.HighWatermark.set(partition, update.highWaterMark)
	m.LastPollOffset.set(partition, update.currentOffset)

	commitOffset, exists := m.CommitOffset.values[partition]
	if !exists {
		// offset of the first message is 0. To clear difference between 2 cases:
		// - first commit: commitOffset = 0
		// - no commit: commitOffset not exists but default value is 0
		// Must consider default value as -1
		commitOffset = -1
	}
	m.Lag.set(partition, update.highWaterMark-1-commitOffset)
}

func (m *Metrics) updateMetricsOnCommit(update *commitMetricsUpdate) {
	m.mu.Lock()
	defer m.mu.Unlock()

	partition := update.partition

	m.CommitCount.inc(partition)
	m.CommitOffset.set(partition, update.commitOffset)

	hwm, exists := m.HighWatermark.values[partition]
	if !exists {
		return
	}

	m.Lag.set(partition, hwm-1-update.commitOffset)
}

func (m *Metrics) snapshot() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Metrics{
		Topic:                m.Topic,
		HighWatermark:        m.HighWatermark.copy(),
		LastPollOffset:       m.LastPollOffset.copy(),
		CommitOffset:         m.CommitOffset.copy(),
		Lag:                  m.Lag.copy(),
		ConsumedMessageCount: m.ConsumedMessageCount.copy(),
		AcceptedMessageCount: m.AcceptedMessageCount.copy(),
		FilteredMessageCount: m.FilteredMessageCount.copy(),
		CommitCount:          m.CommitCount.copy(),
	}
}

func (m *Metrics) clean() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.HighWatermark.drop()
	m.LastPollOffset.drop()
	m.CommitOffset.drop()
	m.Lag.drop()
	m.ConsumedMessageCount.drop()
	m.AcceptedMessageCount.drop()
	m.FilteredMessageCount.drop()
	m.CommitCount.drop()
}
