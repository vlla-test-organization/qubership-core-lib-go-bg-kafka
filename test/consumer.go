package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	bgStateMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversion"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	bgKafka "github.com/netcracker/qubership-core-lib-go-maas-bg-kafka/v3"
	"github.com/stretchr/testify/require"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("test-maas-bg-kafka-consumer")
}

var (
	v1              = bgStateMonitor.NewVersionMust("v1")
	v2              = bgStateMonitor.NewVersionMust("v2")
	namespace1      = "test-ns-1"
	namespace2      = "test-ns-2"
	pollTimeout     = 10 * time.Second
	noRecordTimeout = 3 * time.Second
)

type Writer interface {
	Write(ctx context.Context, message bgKafka.Message) error
}

func RunBGConsumerTest(t *testing.T, topic string,
	bgConsumerSupplier func(options []bgKafka.Option) (*bgKafka.BgConsumer, error),
	writerSupplier func() (Writer, error),
	createTopic func(replicationFactor int) error) {
	initialLogLevel := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", initialLogLevel)
	os.Setenv("LOG_LEVEL", "debug")

	ctx := context.Background()
	assertions := require.New(t)
	configloader.InitWithSourcesArray([]*configloader.PropertySource{configloader.EnvPropertySource()})
	ctxmanager.Register(baseproviders.Get())

	updateTime := time.Date(2018, 12, 3, 19, 34, 50, 0, time.UTC)
	bgState1 := bgStateMonitor.BlueGreenState{
		Current:    bgStateMonitor.NamespaceVersion{Namespace: namespace1, Version: v1, State: bgStateMonitor.StateActive},
		Sibling:    &bgStateMonitor.NamespaceVersion{Namespace: namespace2, Version: v2, State: bgStateMonitor.StateCandidate},
		UpdateTime: updateTime,
	}
	bgState2 := bgStateMonitor.BlueGreenState{
		Current:    bgStateMonitor.NamespaceVersion{Namespace: namespace2, Version: v2, State: bgStateMonitor.StateCandidate},
		Sibling:    &bgStateMonitor.NamespaceVersion{Namespace: namespace1, Version: v1, State: bgStateMonitor.StateActive},
		UpdateTime: updateTime,
	}

	statePublisherActive, err := bgStateMonitor.NewInMemoryPublisher(bgState1)
	assertions.NoError(err)
	statePublisherCandidate, err := bgStateMonitor.NewInMemoryPublisher(bgState2)
	assertions.NoError(err)

	consumerActive, err := bgConsumerSupplier([]bgKafka.Option{
		bgKafka.WithBlueGreenStatePublisher(statePublisherActive),
		bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption),
	})
	assertions.NoError(err)

	consumerCandidate, err := bgConsumerSupplier([]bgKafka.Option{
		bgKafka.WithBlueGreenStatePublisher(statePublisherCandidate),
		bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption),
		bgKafka.WithCandidateOffsetSetupStrategy(bgKafka.StrategyLatest),
	})
	assertions.NoError(err)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " get records by active version and commit only first record")
	logger.InfoC(ctx, " =========================================================================")

	err = createTopic(1)
	assertions.NoError(err)

	logger.InfoC(ctx, "created topic '%s'", topic)

	writer, err := writerSupplier()
	assertions.NoError(err)

	writeMessage(assertions, ctx, writer,
		newMessage("1", "order#1"),
		newMessage("2", "order#2"),
		newMessage("3", "order#3"),
	)

	logger.InfoC(ctx, "polling messages from active consumer")

	record1A, err := consumerActive.Poll(ctx, pollTimeout)

	admin, err := consumerActive.Config.AdminSupplier()
	dumpOffsets(ctx, admin)

	assertions.NoError(err)
	assertions.NotNil(record1A)
	record2A, err := consumerActive.Poll(ctx, pollTimeout)
	assertions.NoError(err)
	assertions.NotNil(record2A)
	record3A, err := consumerActive.Poll(ctx, pollTimeout)
	assertions.NoError(err)
	assertions.NotNil(record3A)
	_, err = consumerActive.Poll(ctx, noRecordTimeout)
	assertions.ErrorIs(err, context.DeadlineExceeded)

	assertions.Equal("order#1", string(record1A.Message.Value()))
	assertions.Equal("order#2", string(record2A.Message.Value()))
	assertions.Equal("order#3", string(record3A.Message.Value()))

	// commit only first record offset
	assertions.NotNil(record1A.Marker)
	err = consumerActive.Commit(ctx, record1A.Marker)
	assertions.NoError(err)
	dumpOffsets(ctx, admin)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " poll from candidate consumer")
	logger.InfoC(ctx, " =========================================================================")

	// no messages in records because of defined initial Offset policy
	_, err = consumerCandidate.Poll(ctx, noRecordTimeout)
	assertions.ErrorIs(err, context.DeadlineExceeded)
	dumpOffsets(ctx, admin)

	// add some more records
	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " add some more records")
	logger.InfoC(ctx, " =========================================================================")

	writeMessage(assertions, ctx, writer,
		newMessage("4", "order#4", newHeader(xversion.X_VERSION_HEADER_NAME, v1.Value)),
		newMessage("5", "order#5", newHeader(xversion.X_VERSION_HEADER_NAME, v2.Value)),
		newMessage("6", "order#6"),
	)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " promote ")
	logger.InfoC(ctx, " =========================================================================")

	updateTime = time.Date(2018, 12, 5, 19, 34, 50, 0, time.UTC)
	bgState1 = bgStateMonitor.BlueGreenState{
		Current:    bgStateMonitor.NamespaceVersion{Namespace: namespace1, Version: v1, State: bgStateMonitor.StateLegacy},
		Sibling:    &bgStateMonitor.NamespaceVersion{Namespace: namespace2, Version: v2, State: bgStateMonitor.StateActive},
		UpdateTime: updateTime,
	}
	bgState2 = bgStateMonitor.BlueGreenState{
		Current:    bgStateMonitor.NamespaceVersion{Namespace: namespace2, Version: v2, State: bgStateMonitor.StateActive},
		Sibling:    &bgStateMonitor.NamespaceVersion{Namespace: namespace1, Version: v1, State: bgStateMonitor.StateLegacy},
		UpdateTime: updateTime,
	}

	statePublisherActive.SetState(bgState1)
	statePublisherCandidate.SetState(bgState2)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " poll by legacy consumer ")
	logger.InfoC(ctx, " =========================================================================")

	r_la1, err := consumerActive.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Nil(r_la1.Message) // record with key "2" must be filtered out

	r_la2, err := consumerActive.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Nil(r_la2.Message) // record with key "3" must be filtered out

	r_la3, err := consumerActive.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.NotNil(r_la3.Message) // record with key "4" has x-version = v1 so should be accepted
	assertions.Equal("4", string(r_la3.Message.Key()))

	r_la4, err := consumerActive.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Nil(r_la4.Message) // record with key "5" must be filtered out

	r_la5, err := consumerActive.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Nil(r_la5.Message) // record with key "6" must be filtered out

	_, err = consumerActive.Poll(ctx, noRecordTimeout)
	assertions.ErrorIs(err, context.DeadlineExceeded)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " poll by active consumer ")
	logger.InfoC(ctx, " =========================================================================")

	r_al1, err := consumerCandidate.Poll(ctx, pollTimeout)
	assertions.NoError(err)
	assertions.Equal("2", string(r_al1.Message.Key()))
	dumpOffsets(ctx, admin)

	r_al3, err := consumerCandidate.Poll(ctx, pollTimeout)
	assertions.NoError(err)
	assertions.Equal("3", string(r_al3.Message.Key()))

	r_al4, err := consumerCandidate.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Nil(r_al4.Message) // record with key "4" must be filtered out

	r_al5, err := consumerCandidate.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Equal("5", string(r_al5.Message.Key()))

	r_al6, err := consumerCandidate.Poll(ctx, pollTimeout)
	dumpOffsets(ctx, admin)
	assertions.NoError(err)
	assertions.Equal("6", string(r_al6.Message.Key()))

	_, err = consumerCandidate.Poll(ctx, noRecordTimeout)
	assertions.ErrorIs(err, context.DeadlineExceeded)
}

func writeMessage(assertions *require.Assertions, ctx context.Context, writer Writer, msgs ...bgKafka.Message) {
	for _, msg := range msgs {
		var headers []string
		for _, h := range msg.Headers() {
			headers = append(headers, h.Key+": "+string(h.Value))
		}
		logger.InfoC(ctx, "sending message: {key: %s, value: %s, headers: %v}",
			string(msg.Key()), string(msg.Value()), headers)
		err := writer.Write(ctx, msg)
		assertions.NoError(err)
	}
}

type KafkaMsg struct {
	topic     string
	offset    int64
	headers   []bgKafka.Header
	key       []byte
	value     []byte
	partition int
	nativeMsg any
}

func (k KafkaMsg) Topic() string {
	return k.topic
}

func (k KafkaMsg) Offset() int64 {
	return k.offset
}

func (k KafkaMsg) Headers() []bgKafka.Header {
	return k.headers
}

func (k KafkaMsg) Key() []byte {
	return k.key
}

func (k KafkaMsg) Value() []byte {
	return k.value
}

func (k KafkaMsg) Partition() int {
	return k.partition
}

func (k KafkaMsg) NativeMsg() any {
	return k.nativeMsg
}

func newMessage(key, value string, headers ...bgKafka.Header) bgKafka.Message {
	return KafkaMsg{
		key:     []byte(key),
		value:   []byte(value),
		headers: headers,
	}
}

func newHeader(key, value string) bgKafka.Header {
	return bgKafka.Header{
		Key:   key,
		Value: []byte(value),
	}
}

func dumpOffsets(ctx context.Context, admin bgKafka.NativeAdminAdapter) {
	logs := ""
	consumerGroups, _ := admin.ListConsumerGroups(ctx)
	for _, cg := range consumerGroups {
		offsets, _ := admin.ListConsumerGroupOffsets(ctx, cg.GroupId)
		logs += fmt.Sprintf("\nOffset for: '%s' = %+v", cg.GroupId, offsets)
	}
	logger.InfoC(ctx, "Offsets:%s", logs)
}
