package blue_green_kafka

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetXVersion(t *testing.T) {
	assertions := require.New(t)
	headers := []Header{
		{Key: "x-version", Value: []byte("v1")},
	}
	version := getXVersion(headers)
	assertions.Equal("v1", version)
}

func TestGetXVersionLowerCase(t *testing.T) {
	assertions := require.New(t)
	headers := []Header{
		{Key: "X-Version", Value: []byte("v1")},
	}
	version := getXVersion(headers)
	assertions.Equal("v1", version)
}
