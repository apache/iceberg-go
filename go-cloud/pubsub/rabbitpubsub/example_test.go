// Copyright 2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rabbitpubsub_test

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/rabbitpubsub"
)

func ExampleOpenTopic() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	rabbitConn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer rabbitConn.Close()
	topic := rabbitpubsub.OpenTopic(rabbitConn, "myexchange", nil)
	defer topic.Shutdown(ctx)
}

func ExampleOpenSubscription() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	rabbitConn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer rabbitConn.Close()
	subscription := rabbitpubsub.OpenSubscription(rabbitConn, "myqueue", nil)
	defer subscription.Shutdown(ctx)
}

func Example_openTopicFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/rabbitpubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenTopic creates a *pubsub.Topic from a URL.
	// This URL will Dial the RabbitMQ server at the URL in the environment
	// variable RABBIT_SERVER_URL and open the exchange "myexchange".
	topic, err := pubsub.OpenTopic(ctx, "rabbit://myexchange")
	if err != nil {
		log.Fatal(err)
	}
	defer topic.Shutdown(ctx)
}

func Example_openSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/rabbitpubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the RabbitMQ server at the URL in the environment
	// variable RABBIT_SERVER_URL and open the queue "myqueue".
	subscription, err := pubsub.OpenSubscription(ctx, "rabbit://myqueue")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}
