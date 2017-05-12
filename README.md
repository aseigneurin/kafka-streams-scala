# kafka-streams-scala

This is a thin Scala wrapper for the [Kafka Streams API](https://kafka.apache.org/documentation/streams). It does not intend to provide a Scala-idiomatic API, but rather intends to make the original API simpler to use from Scala. In particular, it provides the following adjustements:

- Scala lambda expressions can be used directly
- when aggregating and counting, counts are converted from Java `Long`s to Scala `Long`s
- when using a `flatMap` operation, this lets you use a Scala `Iterable`
- `Serde`s (Serializers/Deserializers) can be implicitly found in the scope

This API also contains a few `Serde`s (Serializers/Deserializers):

- to convert Scala `Int`/`Long`/`Double` to/from their binary representation
- to convert Scala `Int`/`Long`/`Double` to/from string representation
- to convert case classes to/from JSON

## Usage of the Kafka Streams API in Scala

The main objects are:

- `KStreamsBuilderS` as the entry point to build streams or tables
- `KStreamS` as a wrapper around `KStream`
- `KGroupedStreamS` as a wrapper around `KGroupedStream`
- `KTableS` as a wrapper around `KTable`
- `KGroupedTable` as a wrapper around `KGroupedTable`

### Using the builder

With the original Java API, you would create an instance of `KStreamBuilder`, then use it to create streams or tables. Here, `KStreamsBuilderS` is an `object` thqt can be used directly:

```scala
val stream: KStreamS[String, String] = KStreamBuilderS.stream[String, String]("my-stream")

val table: KTableS[String, String] = KStreamBuilderS.table[String, String]("my-table")
```

When starting the application, you just need to unwrap the `KStreamBuilder` by calling `KStreamBuilderS.inner`:

```scala
val streams = new KafkaStreams(KStreamBuilderS.inner, config)
```

### Serdes (declare them as implicit)

It is a common mistake to forget to specify `Serde`s when using the Java API, then resulting in class cast errors when objects are serialized or deserialized.

To work around this issue, this API requires `Serde`s to be used. Most of the times, it is enough to declare your `Serde`s as `implicit` values, and they will be picked up automatically:

```scala
implicit val stringSerde: Serde[String] = Serdes.String()
implicit val userSerde: Serde[User] = new MyUserSerde

val usersByIdStream = KStreamBuilderS.stream[String, User]("users-by-id")
```

Resolution is based on the type of the object to serialize/deserialize, so make sure you have a `Serve` of the appropriate type. If not, you should see an error such as:

```
Error:(87, 80) could not find implicit value for parameter keySerde: org.apache.kafka.common.serialization.Serde[String]
```

If, on the other hand, you have multiple `Serde`s for the same type, you might see the following error:

```
Error:(88, 80) ambiguous implicit values:
 both value stringSerde2 of type org.apache.kafka.common.serialization.Serde[String]
 and value stringSerde1 of type org.apache.kafka.common.serialization.Serde[String]
 match expected type org.apache.kafka.common.serialization.Serde[String]
```

In this case, just pass the `Serde` explicitly:

```scala
val usersByIdStream = KStreamBuilderS.stream[String, User]("users-by-id")(stringSerde, userSerde)
```

## Usage of the Serdes in Scala

To convert Scala `Int`/`Long`/`Double` to/from their binary representation:

```scala
import com.github.aseigneurin.kafka.serialization.scala._

implicit val intSerde = IntAsStringSerde
implicit val longSerde = LongAsStringSerde
implicit val doubleSerde = DoubleAsStringSerde
```

To convert Scala `Int`/`Long`/`Double` to/from string representation:

```scala
import com.github.aseigneurin.kafka.serialization.scala._

implicit val intSerde = IntSerde
implicit val longSerde = LongSerde
implicit val doubleSerde = DoubleSerde
```

To convert case classes to/from JSON:

- define a `case class`
- create an instance of `JsonSerde` with the case class as the generic type

Example:

```scala
import com.github.aseigneurin.kafka.serialization.scala._

case class User(name: String)

implicit val stringSerde = Serdes.String
implicit val userSerde = new JsonSerde[User]

// read JSON -> case class
KStreamBuilderS.stream[String, User]("users")
  .mapValues { user => user.name }
  .to("names")

// write case class -> JSON
KStreamBuilderS.stream[String, String]("names")
  .mapValues { name => User(name) }
  .to("users")
```

## Example

This repository contains [a Scala version](https://github.com/aseigneurin/kafka-streams-scala/blob/master/src/test/scala/com/github/aseigneurin/kafka/streams/scala/WordCountDemo.scala) of [the Java Word Count Demo](https://github.com/apache/kafka/blob/trunk/streams/examples/src/main/java/org/apache/kafka/streams/examples/wordcount/WordCountDemo.java).

Here is the code to implement a word count:

```scala
val props = new Properties()
// ...

implicit val stringSerde = Serdes.String
implicit val longSerde = LongAsStringSerde

val source = KStreamBuilderS.stream[String, String]("streams-file-input")

val counts: KTableS[String, Long] = source
  .flatMapValues { value => value.toLowerCase(Locale.getDefault).split(" ") }
  .map { (_, value) => (value, value) }
  .groupByKey
  .count("Counts")

counts.to("streams-wordcount-output")

val streams = new KafkaStreams(KStreamBuilderS.inner, props)
streams.start()
```
