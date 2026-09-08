// Package easykafkaconfig projects compacted Kafka topics into typed, thread-safe,
// in-memory maps and keeps them live for the lifetime of the process.
//
// It exists for one job: configuration distributed over compacted topics, where a
// record is an absolute upsert for its key and an empty record is a delete. A
// service declares one binding per topic, blocks once at startup until every
// topic has been read to its end, and from then on performs O(1), type-safe
// lookups while the library keeps applying changes in the background.
//
// For how it works underneath — the lifecycle, the guarantees and their limits,
// the failure behaviour and the design decisions — see developer-doc.md in the
// repository.
//
// # Getting started
//
// See the package example for a complete service, from the process context
// through to shutdown. It is compiled with the tests, so unlike a snippet in a
// comment it cannot drift away from the API it demonstrates.
//
// The shape of it: build a Loader, Bind one Binding per topic, call Start and
// let it block, then read from the typed stores it returned. On the way out,
// cancel the context Start was given and call WaitUntilStopped.
//
// # What it guarantees
//
// Start returns nil only when every bound topic has been read to its end, so a
// service that gets past it holds complete configuration rather than whatever
// had arrived by then. Warm-up is all-or-nothing: one topic failing fails the
// call, with every failure reported together rather than only the first.
//
// Offsets are never committed. A restart therefore rebuilds the stores by
// construction rather than by luck, which is what makes them reproducible.
//
// Partitions are assigned explicitly and no consumer group is ever joined, so
// every replica reads every partition, there is no rebalance, and adding a
// replica costs the cluster nothing in coordination.
//
// # What it does not do
//
// It reads topics into maps. It does not join them, aggregate them, window them
// or write anything back, and it holds each store wholly in memory — so a topic
// larger than the memory available to a replica is out of scope, as is any state
// derived from more than one topic.
//
// The library never terminates the process. An empty required topic, a payload
// that will not decode, a broker that has gone for good: each surfaces as an
// error, as Err, or as an Observer callback, and the service decides what to do.
//
// # Warm-up detection
//
// Kafka gives a consumer no end-of-topic signal, so the end has to be inferred.
// PartitionEOF does it by waiting for the broker to report every assigned
// partition exhausted, which makes no timing assumption and reports an empty
// topic in milliseconds rather than after a timeout.
//
// It is currently the only detector, and the default. WithInitialLoadDetector
// exists because alternatives are designed and may yet be built, and because it
// carries the warm-up poll timeout.
//
// # Relationship to easykafka-go
//
// This library talks to confluent-kafka-go directly and does not depend on
// easykafka-go. Reading a compacted topic needs the record key, no offset commits,
// read-to-end detection and a poll timeout that changes between startup and steady
// state — none of which fits a handler-based consumer built around retry and
// dead-letter semantics. easykafka-go remains the right choice for event consumers.
package easykafkaconfig
