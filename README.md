# kafka-streams-scala

This is a thin Scala wrapper for the [Kafka Streams API](https://kafka.apache.org/documentation/streams). It does not intend to provide a Scala-idiomatic API, but rather intends to make the original API simpler to use from Scala. In particular, it provides the following adjustements:

- Scala lambda expressions can be used directly
- when aggregating and counting, counts are converted from Java `Long`s to Scala `Long`s
- `Serde`s (Serializers/Deserializers) can be implicitly found in the scope
