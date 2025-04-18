package test

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"os"
	"testing"
)

var (
	kafkaPort       = "9092"
	kafkaPortDocker = nat.Port(fmt.Sprintf("%s/tcp", kafkaPort))
)

func setTestDocker(t *testing.T) {
	if testDockerUrl := os.Getenv("TEST_DOCKER_URL"); testDockerUrl != "" {
		err := os.Setenv("DOCKER_HOST", testDockerUrl)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Logf("set DOCKER_HOST to value from 'TEST_DOCKER_URL' as '%s'.", testDockerUrl)
	} else {
		t.Logf("TEST_DOCKER_URL is empty")
	}
}

func StartContainers(t *testing.T) ([]string, error) {
	ctx := context.Background()
	setTestDocker(t)

	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		return nil, err
	}
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		return nil, err
	}
	t.Logf("kafka brokers: %v", brokers)
	return brokers, nil
}
