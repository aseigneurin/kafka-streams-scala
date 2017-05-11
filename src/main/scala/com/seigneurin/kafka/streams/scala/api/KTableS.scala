package com.seigneurin.kafka.streams.scala.api

import com.seigneurin.kafka.streams.scala.api.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.StreamPartitioner

class KTableS[K, V](val inner: KTable[K, V]) {

  def filter(predicate: (K, V) => Boolean): KTableS[K, V] = {
    val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
    inner.filter(predicateJ)
  }

  def filterNot(predicate: (K, V) => Boolean): KTableS[K, V] = {
    val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
    inner.filterNot(predicateJ)
  }

  def mapValues[VR](mapper: (V) => VR): KTable[K, VR] = {
    def mapperJ: ValueMapper[V, VR] = (v) => mapper(v)

    inner.mapValues(mapperJ)
  }

  def print() =
    inner.print()

  def print(streamName: String) =
    inner.print(streamName)

  def print(keySerde: Serde[K], valSerde: Serde[V]) =
    inner.print(keySerde, valSerde)

  def print(keySerde: Serde[K], valSerde: Serde[V], streamName: String) =
    inner.print(keySerde, valSerde, streamName)

  def writeAsText(filePath: String)
                 (implicit keySerde: Serde[K], valSerde: Serde[V]) = {
    inner.writeAsText(filePath, keySerde, valSerde)
  }

  def writeAsText(filePath: String,
                  streamName: String)
                 (implicit keySerde: Serde[K], valSerde: Serde[V]) = {
    inner.writeAsText(filePath, streamName, keySerde, valSerde)
  }

  def foreach(action: (K, V) => Unit): Unit = {
    val actionJ: ForeachAction[_ >: K, _ >: V] = (k: K, v: V) => action(k, v)
    inner.foreach(actionJ)
  }

  def toStream: KStreamS[K, V] =
    inner.toStream

  def toStream[KR](mapper: (K, V) => KR): KStreamS[KR, V] = {
    val mapperJ: KeyValueMapper[K, V, KR] = (k: K, v: V) => mapper(k, v)
    inner.toStream[KR](mapperJ)
  }

  def through(topic: String,
              storeName: String)
             (implicit keySerde: Serde[K], valSerde: Serde[V]): KTableS[K, V] = {
    inner.through(keySerde, valSerde, topic, storeName)
  }

  def through(partitioner: (K, V, Int) => Int,
              topic: String,
              storeName: String)
             (implicit keySerde: Serde[K], valSerde: Serde[V]): KTableS[K, V] = {
    val partitionerJ: StreamPartitioner[K, V] =
      (key: K, value: V, numPartitions: Int) => partitioner(key, value, numPartitions)
    inner.through(keySerde, valSerde, partitionerJ, topic, storeName)
  }

  def to(topic: String)
        (implicit keySerde: Serde[K], valSerde: Serde[V]) = {
    inner.to(keySerde, valSerde, topic)
  }

  def to(partitioner: (K, V, Int) => Int,
         topic: String)
        (implicit keySerde: Serde[K], valSerde: Serde[V]) = {
    val partitionerJ: StreamPartitioner[K, V] =
      (key: K, value: V, numPartitions: Int) => partitioner(key, value, numPartitions)
    inner.to(keySerde, valSerde, partitionerJ, topic)
  }

  def groupBy[KR, VR](selector: (K, V) => (KR, VR)): KGroupedTableS[KR, VR] = {
    val selectorJ: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k: K, v: V) => {
      val res = selector(k, v)
      new KeyValue[KR, VR](res._1, res._2)
    }
    inner.groupBy(selectorJ)
  }

  def groupBy[KR, VR](selector: (K, V) => (KR, VR),
                      keySerde: Serde[KR],
                      valueSerde: Serde[VR]): KGroupedTableS[KR, VR] = {
    val selectorJ: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k: K, v: V) => {
      val res = selector(k, v)
      new KeyValue[KR, VR](res._1, res._2)
    }
    inner.groupBy(selectorJ, keySerde, valueSerde)
  }

  def join[VO, VR](other: KTableS[K, VO],
                   joiner: (V, VO) => VR): KTableS[K, VR] = {
    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VO, VR](other.inner, joinerJ)
  }

  def leftJoin[VO, VR](other: KTableS[K, VO],
                       joiner: (V, VO) => VR): KTableS[K, VR] = {
    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VO, VR](other.inner, joinerJ)
  }

  def outerJoin[VO, VR](other: KTableS[K, VO],
                        joiner: (V, VO) => VR): KTableS[K, VR] = {
    val joinerJ: ValueJoiner[V, VO, VR] = (v1, v2) => joiner(v1, v2)
    inner.outerJoin[VO, VR](other.inner, joinerJ)
  }

  def getStoreName: String =
    inner.getStoreName

}
