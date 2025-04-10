package blue_green_kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/netcracker/qubership-core-lib-go-rest-utils/v2/consul-propertysource"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
)

type BgKafkaConsumerConfig struct {
	Topic                        func() string
	AdminSupplier                func() (NativeAdminAdapter, error)
	ActiveOffsetSetupStrategy    func() OffsetSetupStrategy
	CandidateOffsetSetupStrategy func() OffsetSetupStrategy
	ConsumerSupplier             func(groupId string) (Consumer, error)
	GroupIdPrefix                func() string
	ConsistencyMode              func() ConsumerConsistencyMode
	BlueGreenStatePublisher      func() (BGStatePublisher, error)
	ReadTimeout                  func() time.Duration
}

type Option func(options *BgKafkaConsumerConfig)

func WithBlueGreenStatePublisher(statePublisher BGStatePublisher) Option {
	return func(config *BgKafkaConsumerConfig) {
		if statePublisher == nil {
			panic("statePublisher cannot be nil")
		}
		config.BlueGreenStatePublisher = func() (BGStatePublisher, error) { return statePublisher, nil }
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(config *BgKafkaConsumerConfig) {
		config.ReadTimeout = func() time.Duration { return timeout }
	}
}

func WithConsistencyMode(mode ConsumerConsistencyMode) Option {
	return func(config *BgKafkaConsumerConfig) {
		config.ConsistencyMode = func() ConsumerConsistencyMode { return mode }
	}
}

func WithActiveOffsetSetupStrategy(strategy OffsetSetupStrategy) Option {
	return func(config *BgKafkaConsumerConfig) {
		config.ActiveOffsetSetupStrategy = func() OffsetSetupStrategy { return strategy }
	}
}

func WithCandidateOffsetSetupStrategy(strategy OffsetSetupStrategy) Option {
	return func(config *BgKafkaConsumerConfig) {
		config.CandidateOffsetSetupStrategy = func() OffsetSetupStrategy { return strategy }
	}
}

func getMicroserviceName() string {
	return configloader.GetKoanf().MustString("microservice.name")
}

func getDeploymentResourceName() string {
	return configloader.GetKoanf().MustString("deployment.resource.name")
}

func getNamespace() string {
	return configloader.GetKoanf().MustString("microservice.namespace")
}

func getConsulUrl() string {
	return configloader.GetKoanf().String("consul.url")
}

func getAuthSupplier() func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
	    tokenProvider := serviceloader.MustLoad[security.TokenProvider]()
		return tokenProvider.GetToken(ctx)
	}
}

func getConsulTokenSupplier(ctx context.Context, consulUrl string, namespace string) (func(ctx context.Context) (string, error), error) {
	consulClient := consul.NewClient(consul.ClientConfig{
		Address:   consulUrl,
		Namespace: namespace,
		Ctx:       ctx,
	})
	err := consulClient.Login()
	if err != nil {
		return nil, fmt.Errorf("failed to start consulClient: %w", err)
	}
	return func(ctx context.Context) (string, error) {
		return consulClient.SecretId(), nil
	}, nil
}
