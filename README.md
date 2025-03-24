# Blue-Green for Kafka clients

<!-- TOC -->
* [Blue-Green for Kafka clients](#blue-green-for-kafka-clients)
  * [Blue Green Version tracking filter](#blue-green-version-tracking-filter)
<!-- TOC -->

This library provides abstract implementation to consume messages from Kafka in Cloud-Core Blue Green scenarios

The library is extended by adapters, refer to the documentation of particular adapter:
* [segmentio adapter](https://github.com/netcracker/qubership-core-lib-go-maas-bg-segmentio)

## Blue Green Version tracking filter
This library provides implementation of Blue Green version filter by [filter.go](filter.go).  
This implementation depends on [blue-green-state-monitor-go](https://github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2) library.

Usage example:
```go
import (
    "context"
    "fmt"
    bgState "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
    bgKafka "github.com/netcracker/qubership-core-lib-go-maas-bg-kafka/v3"
)

func FilterDemo(statePublisher *bgState.ConsulBlueGreenStatePublisher) {
    filter := bgKafka.NewTrackingVersionFilter(context.Background(), statePublisher)
    result = filter.Test("v1")
}

```

