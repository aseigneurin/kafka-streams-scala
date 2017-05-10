package com.seigneurin.kafka.streams.scala.api

import java.util.regex.Pattern

import com.seigneurin.kafka.streams.scala.api.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, KStream, KStreamBuilder}
import org.apache.kafka.streams.processor.TopologyBuilder

object KStreamBuilderS {

  val inner = new KStreamBuilder

  def stream[K, V](topics: String*): KStreamS[K, V] =
    inner.stream[K, V](topics: _*)

  def stream[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                   topics: String*): KStream[K, V] =
    inner.stream[K, V](offsetReset, topics: _*)

  def stream[K, V](topicPattern: Pattern): KStreamS[K, V] =
    inner.stream[K, V](topicPattern)

  def stream[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                   topicPattern: Pattern): KStreamS[K, V] =
    inner.stream[K, V](offsetReset, topicPattern)

  def stream[K, V](keySerde: Serde[K],
                   valSerde: Serde[V],
                   topics: String*): KStreamS[K, V] =
    inner.stream[K, V](keySerde, valSerde, topics: _*)

  def stream[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                   keySerde: Serde[K],
                   valSerde: Serde[V],
                   topics: String*): KStreamS[K, V] =
    inner.stream[K, V](offsetReset, keySerde, valSerde, topics: _*)

  def stream[K, V](keySerde: Serde[K],
                   valSerde: Serde[V],
                   topicPattern: Pattern): KStreamS[K, V] =
    inner.stream[K, V](keySerde, valSerde, topicPattern)

  def stream[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                   keySerde: Serde[K],
                   valSerde: Serde[V],
                   topicPattern: Pattern): KStreamS[K, V] =
    inner.stream[K, V](offsetReset, keySerde, valSerde, topicPattern)

  def table[K, V](topic: String,
                  storeName: String): KTableS[K, V] =
    inner.table[K, V](topic, storeName)

  def table[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                  topic: String,
                  storeName: String): KTableS[K, V] =
    inner.table[K, V](offsetReset, topic, storeName)

  def table[K, V](keySerde: Serde[K],
                  valSerde: Serde[V],
                  topic: String,
                  storeName: String): KTableS[K, V] =
    inner.table[K, V](keySerde, valSerde, topic, storeName)

  def table[K, V](offsetReset: TopologyBuilder.AutoOffsetReset,
                  keySerde: Serde[K],
                  valSerde: Serde[V],
                  topic: String,
                  storeName: String): KTableS[K, V] =
    inner.table[K, V](offsetReset, keySerde, valSerde, topic, storeName)

  def globalTable[K, V](topic: String, storeName: String): GlobalKTable[K, V] =
    inner.globalTable(topic, storeName)

  def globalTable[K, V](keySerde: Serde[K],
                        valSerde: Serde[V],
                        topic: String,
                        storeName: String): GlobalKTable[K, V] =
    inner.globalTable(keySerde, valSerde, topic, storeName)

  def merge[K, V](streams: KStream[K, V]*): KStream[K, V] =
    inner.merge(streams: _*)

}
