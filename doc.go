// Package easykafkaconfig projects compacted Kafka topics into typed, thread-safe,
// in-memory maps and keeps them live for the lifetime of the process.
//
// It exists for one job: configuration distributed over compacted topics, where a
// record is an absolute upsert for its key and an empty record is a delete. A
// service declares one binding per topic, blocks once at startup until every
// topic has been read to its end, and from then on performs O(1), type-safe
// lookups while the library keeps applying changes in the background.
//
// # Status
//
// Work in progress. Store, Binding and the codecs (StringKey, IntKey, Int64Key,
// JSONValue and the tombstone policies) are implemented and tested. Loader, the
// driver layer and the warm-up detectors are not, so nothing reads a topic yet —
// the sketch below is the target shape.
//
// # Intended shape
//
//	loader, err := ekconfig.NewLoader(
//	    ekconfig.WithBrokers("localhost:9092"),
//	    ekconfig.WithClientGroupID("my-config-reader"),
//	)
//	if err != nil {
//	    return err
//	}
//
//	players := loader.Bind(ekconfig.Binding[string, PlayerConfig]{
//	    Name:        "PlayerConfig",
//	    Topic:       "player-config.compact",
//	    DecodeKey:   ekconfig.StringKey,
//	    DecodeValue: ekconfig.JSONValue[PlayerConfig],
//	})
//
//	if err := loader.Start(ctx); err != nil {   // blocks until every topic is drained
//	    return err
//	}
//	defer loader.Close(context.WithoutCancel(ctx))
//
//	cfg := players.GetOrNil("player-42")        // typed, no assertions, no lock
//
// # Relationship to easykafka-go
//
// This library talks to confluent-kafka-go directly and does not depend on
// easykafka-go. Reading a compacted topic needs the record key, no offset commits,
// read-to-end detection and a poll timeout that changes between startup and steady
// state — none of which fits a handler-based consumer built around retry and
// dead-letter semantics. easykafka-go remains the right choice for event consumers.
package easykafkaconfig
