package com.seigneurin.kafka.streams.scala.api

import org.apache.kafka.streams.kstream.{KGroupedStream, KStream, KTable}

object ImplicitConversions {

  implicit def wrapKStream[K, V](kstream: KStream[K, V]): KStreamS[K, V] =
    new KStreamS[K, V](kstream)

  implicit def wrapKGroupedStream[K, V](kGroupedStream: KGroupedStream[K, V]): KGroupedStreamS[K, V] =
    new KGroupedStreamS[K, V](kGroupedStream)

  implicit def wrapKTable[K, V](ktable: KTable[K, V]): KTableS[K, V] =
    new KTableS[K, V](ktable)

}
