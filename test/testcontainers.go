package test

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"net/url"
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
	kafkaHost := "127.0.0.1"
	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost != "" {
		dockerUrl, err := url.Parse(dockerHost)
		if err != nil {
			return nil, err
		}
		kafkaHost = dockerUrl.Hostname()
		t.Logf("found DOCKER_HOST='%s'. Setting kafkaHost='%s'", dockerHost, kafkaHost)
	}
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
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
