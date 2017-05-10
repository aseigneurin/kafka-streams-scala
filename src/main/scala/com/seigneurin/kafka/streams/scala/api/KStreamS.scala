package com.seigneurin.kafka.streams.scala.api

import com.seigneurin.kafka.streams.scala.api.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.StreamPartitioner

import scala.collection.JavaConversions._

class KStreamS[K, V](val inner: KStream[K, V]) {

  def filter(predicate: (K, V) => Boolean): KStreamS[K, V] = {
    val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
    inner.filter(predicateJ)
  }

  def filterNot(predicate: (K, V) => Boolean): KStreamS[K, V] = {
    val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
    inner.filterNot(predicateJ)
  }

  def selectKey[KR](mapper: (K, V) => KR): KStream[KR, V] = {
    val mapperJ: KeyValueMapper[K, V, KR] = (k: K, v: V) => mapper(k, v)
    inner.selectKey(mapperJ)
  }

  def map[KR, VR](mapper: (K, V) => (KR, VR)): KStreamS[KR, VR] = {
    val mapperJ: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k: K, v: V) => {
      val res = mapper(k, v)
      new KeyValue[KR, VR](res._1, res._2)
    }
    inner.map[KR, VR](mapperJ)
  }

  def mapValues[VR](mapper: (V => VR)): KStreamS[K, VR] = {
    val mapperJ: ValueMapper[V, VR] = (v: V) => mapper(v)
    val res: KStream[K, VR] = inner.mapValues(mapperJ)
    res
  }

  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): KStream[KR, VR] = {
    val mapperJ: KeyValueMapper[K, V, java.lang.Iterable[KeyValue[KR, VR]]] = (k: K, v: V) => {
      val resTuples: Iterable[(KR, VR)] = mapper(k, v)
      val res: Iterable[KeyValue[KR, VR]] = resTuples.map(t => new KeyValue[KR, VR](t._1, t._2))
      asJavaIterable(res)
    }
    inner.flatMap[KR, VR](mapperJ)
  }

  def flatMapValues[VR](processor: ValueMapper[V, Iterable[VR]]): KStream[K, VR] = {
    val processorJ: ValueMapper[V, java.lang.Iterable[VR]] = (v: V) => {
      val res: Iterable[VR] = processor(v)
      asJavaIterable(res)
    }
    inner.flatMapValues[VR](processorJ)
  }

  def print() =
    inner.print()

  def print(streamName: String) =
    inner.print(streamName)

  def print(keySerde: Serde[K], valSerde: Serde[V]) =
    inner.print(keySerde, valSerde)

  def print(keySerde: Serde[K], valSerde: Serde[V], streamName: String) =
    inner.print(keySerde, valSerde, streamName)

  def writeAsText(filePath: String) =
    inner.writeAsText(filePath)

  def writeAsText(filePath: String, streamName: String) =
    inner.writeAsText(filePath, streamName)

  def writeAsText(filePath: String, keySerde: Serde[K], valSerde: Serde[V]) =
    inner.writeAsText(filePath, keySerde, valSerde)

  def writeAsText(filePath: String, streamName: String, keySerde: Serde[K], valSerde: Serde[V]) =
    inner.writeAsText(filePath, streamName, keySerde, valSerde)

  def foreach(action: (K, V) => Unit): Unit = {
    val actionJ: ForeachAction[_ >: K, _ >: V] = (k: K, v: V) => action(k, v)
    inner.foreach(actionJ)
  }

  def branch(predicates: ((K, V) => Boolean)*): Array[KStream[K, V]] = {
    val predicatesJ = predicates.map(predicate => {
      val predicateJ: Predicate[K, V] = (k, v) => predicate(k, v)
      predicateJ
    })
    inner.branch(predicatesJ: _*)
  }

  def through(topic: String): KStream[K, V] =
    inner.through(topic)

  def through(partitioner: (K, V, Int) => Int,
              topic: String): KStream[K, V] = {
    val partitionerJ: StreamPartitioner[K, V] =
      (key: K, value: V, numPartitions: Int) => partitioner(key, value, numPartitions)
    inner.through(partitionerJ, topic)
  }

  def through(keySerde: Serde[K],
              valSerde: Serde[V],
              topic: String): KStream[K, V] = {
    inner.through(keySerde, valSerde, topic)
  }

  def through(keySerde: Serde[K],
              valSerde: Serde[V],
              partitioner: (K, V, Int) => Int,
              topic: String): KStream[K, V] = {
    val partitionerJ: StreamPartitioner[K, V] =
      (key: K, value: V, numPartitions: Int) => partitioner(key, value, numPartitions)
    inner.through(keySerde, valSerde, partitionerJ, topic)
  }

  def to(topic: String) =
    inner.to(topic)

  def to(partitioner: (K, V, Int) => Int,
         topic: String) = {
    val partitionerJ: StreamPartitioner[K, V] =
      (key: K, value: V, numPartitions: Int) => partitioner(key, value, numPartitions)
    inner.to(partitionerJ, topic)
  }

  def to(keySerde: Serde[K],
         valSerde: Serde[V],
         topic: String) = {
    inner.to(keySerde, valSerde, topic)
  }

  def to(keySerde: Serde[K],
         valSerde: Serde[V],
         partitioner: (K, V, Int) => Int,
         topic: String) = {
    val partitionerJ: StreamPartitioner[K, V] =
      (key: K, value: V, numPartitions: Int) => partitioner(key, value, numPartitions)
    inner.to(keySerde, valSerde, partitionerJ, topic)
  }

  //    def transform[K1, V1](transformerSupplier: TransformerSupplier[_ >: K, _ >: V, KeyValue[K1, V1]],
  //                          stateStoreNames: String*): KStream[K1, V1]
  //
  //    def transformValues[VR](valueTransformerSupplier: ValueTransformerSupplier[_ >: V, _ <: VR],
  //                            stateStoreNames: String*): KStream[K, VR]
  //
  //    def process(processorSupplier: ProcessorSupplier[_ >: K, _ >: V], stateStoreNames: String*)

  def groupByKey: KGroupedStreamS[K, V] =
    inner.groupByKey()

  def groupByKey(keySerde: Serde[K], valSerde: Serde[V]): KGroupedStreamS[K, V] = {
    inner.groupByKey(keySerde, valSerde)
  }

  //  def groupBy[KR](selector: KeyValueMapper[_ >: K, _ >: V, KR]): KGroupedStream[KR, V]
  //
  //  def groupBy[KR](selector: KeyValueMapper[_ >: K, _ >: V, KR],
  //                  keySerde: Serde[KR], valSerde: Serde[V]): KGroupedStream[KR, V]
  //
  //  def join[VO, VR](otherStream: KStream[K, VO],
  //                   joiner: ValueJoiner[_ >: V, _ >: VO, _ <: VR],
  //                   windows: JoinWindows): KStream[K, VR]
  //
  //  def join[VO, VR](otherStream: KStream[K, VO],
  //                   joiner: ValueJoiner[_ >: V, _ >: VO, _ <: VR],
  //                   windows: JoinWindows,
  //                   keySerde: Serde[K],
  //                   thisValueSerde: Serde[V],
  //                   otherValueSerde: Serde[VO]): KStream[K, VR]
  //
  //  def leftJoin[VO, VR](otherStream: KStream[K, VO],
  //                       joiner: ValueJoiner[_ >: V, _ >: VO, _ <: VR],
  //                       windows: JoinWindows): KStream[K, VR]
  //
  //  def leftJoin[VO, VR](otherStream: KStream[K, VO],
  //                       joiner: ValueJoiner[_ >: V, _ >: VO, _ <: VR],
  //                       windows: JoinWindows,
  //                       keySerde: Serde[K],
  //                       thisValSerde: Serde[V],
  //                       otherValueSerde: Serde[VO]): KStream[K, VR]
  //
  //  def outerJoin[VO, VR](otherStream: KStream[K, VO],
  //                        joiner: ValueJoiner[_ >: V, _ >: VO, _ <: VR],
  //                        windows: JoinWindows): KStream[K, VR]
  //
  //  def outerJoin[VO, VR](otherStream: KStream[K, VO],
  //                        joiner: ValueJoiner[_ >: V, _ >: VO, _ <: VR],
  //                        windows: JoinWindows,
  //                        keySerde: Serde[K],
  //                        thisValueSerde: Serde[V],
  //                        otherValueSerde: Serde[VO]): KStream[K, VR]

  def join[VT, VR](table: KTableS[K, VT],
                   joiner: (V, VT) => VR): KStreamS[K, VR] = {
    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VT, VR](table.inner, joinerJ)
  }

  def join[VT, VR](table: KTableS[K, VT],
                   joiner: (V, VT) => VR,
                   keySerde: Serde[K],
                   valSerde: Serde[V]): KStreamS[K, VR] = {
    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.join[VT, VR](table.inner, joinerJ, keySerde, valSerde)
  }

  def leftJoin[VT, VR](table: KTableS[K, VT],
                       joiner: (V, VT) => VR): KStreamS[K, VR] = {
    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VT, VR](table.inner, joinerJ)
  }

  def leftJoin[VT, VR](table: KTableS[K, VT],
                       joiner: (V, VT) => VR,
                       keySerde: Serde[K],
                       valSerde: Serde[V]): KStreamS[K, VR] = {
    val joinerJ: ValueJoiner[V, VT, VR] = (v1, v2) => joiner(v1, v2)
    inner.leftJoin[VT, VR](table.inner, joinerJ, keySerde, valSerde)
  }

  //  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
  //                       keyValueMapper: KeyValueMapper[_ >: K, _ >: V, _ <: GK],
  //                       joiner: ValueJoiner[_ >: V, _ >: GV, _ <: RV]): KStream[K, RV]
  //
  //  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV],
  //                           keyValueMapper: KeyValueMapper[_ >: K, _ >: V, _ <: GK],
  //                           valueJoiner: ValueJoiner[_ >: V, _ >: GV, _ <: RV]): KStream[K, RV]

}
