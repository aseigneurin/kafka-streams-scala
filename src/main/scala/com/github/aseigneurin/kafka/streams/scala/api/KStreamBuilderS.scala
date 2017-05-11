package com.github.aseigneurin.kafka.streams.scala.api

import java.util.regex.Pattern

import com.github.aseigneurin.kafka.streams.scala.api.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, KStreamBuilder}
import org.apache.kafka.streams.processor.TopologyBuilder

object KStreamBuilderS {

  val inner = new KStreamBuilder

  def stream[K, V](topics: String*)
                  (implicit keySerde: Serde[K], valSerde: Serde[V]): KStreamS[K, V] =
    inner.stream[K, V](keySerde, valSerde, topics: _*)

  def stream[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                   topics: String*)
                  (implicit keySerde: Serde[K], valSerde: Serde[V]): KStreamS[K, V] =
    inner.stream[K, V](offsetReset, keySerde, valSerde, topics: _*)

  def stream[K, V](topicPattern: Pattern)
                  (implicit keySerde: Serde[K], valSerde: Serde[V]): KStreamS[K, V] =
    inner.stream[K, V](keySerde, valSerde, topicPattern)

  def stream[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                   topicPattern: Pattern)
                  (implicit keySerde: Serde[K], valSerde: Serde[V]): KStreamS[K, V] =
    inner.stream[K, V](offsetReset, keySerde, valSerde, topicPattern)

  def table[K, V](topic: String,
                  storeName: String)
                 (implicit keySerde: Serde[K], valSerde: Serde[V]): KTableS[K, V] =
    inner.table[K, V](keySerde, valSerde, topic, storeName)

  def table[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                  topic: String,
                  storeName: String)
                 (implicit keySerde: Serde[K], valSerde: Serde[V]): KTableS[K, V] =
    inner.table[K, V](offsetReset, keySerde, valSerde, topic, storeName)


  def globalTable[K, V](topic: String,
                        storeName: String)
                       (implicit keySerde: Serde[K], valSerde: Serde[V]): GlobalKTable[K, V] =
    inner.globalTable(keySerde, valSerde, topic, storeName)

  def merge[K, V](streams: KStreamS[K, V]*): KStreamS[K, V] = {
    val streamsJ = streams.map { streamS => streamS.inner }
    inner.merge(streamsJ: _*)
  }

}
