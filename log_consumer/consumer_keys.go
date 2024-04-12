package log_consumer

import (
	"fmt"
	"strings"
)

type consumerKey string

func createConsumerKey(queue, consumer string) consumerKey {
	return consumerKey(fmt.Sprintf("%s::%s", queue, consumer))
}

func (key consumerKey) parse() (queue, consumer string) {
	parts := strings.SplitN(string(key), "::", 2)

	// Let this explode if the serialization breaks I guess, since it's guarded elsewhere
	return parts[0], parts[1]
}
