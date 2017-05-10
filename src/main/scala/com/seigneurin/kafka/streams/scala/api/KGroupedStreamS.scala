package com.seigneurin.kafka.streams.scala.api

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._

class KGroupedStreamS[K, V](inner: KGroupedStream[K, V]) {

  implicit def wrapKStream[K, V](kstream: KStream[K, V]): KStreamS[K, V] =
    new KStreamS[K, V](kstream)

  implicit def wrapKTable[K, V](ktable: KTable[K, V]): KTableS[K, V] =
    new KTableS[K, V](ktable)

  def count(storeName: String): KTableS[K, Long] = {
    inner.count(storeName)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  //  def count(storeSupplier: StateStoreSupplier[KeyValueStore[K, V]]): KTableS[K, Long] = {
  //    groupedStream.count(storeSupplier)
  //      .mapValues[Long](javaLong => Long.box(javaLong))
  //  }

  def count[W <: Window](windows: Windows[W],
                         storeName: String): KTable[Windowed[K], Long] = {
    inner.count[W](windows, storeName)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  //  def count[W <: Window](windows: Windows[W],
  //                         storeSupplier: StateStoreSupplier[WindowStore[K, V]]): KTable[Windowed[K], Long] = {
  //    groupedStream.count[W](windows, storeSupplier)
  //      .mapValues[Long](javaLong => Long.box(javaLong))
  //  }

  def count(sessionWindows: SessionWindows, storeName: String): KTable[Windowed[K], Long] = {
    inner.count(sessionWindows, storeName)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  //  def count(sessionWindows: SessionWindows,
  //            storeSupplier: StateStoreSupplier[SessionStore[K, V]]): KTable[Windowed[K], Long] = {
  //    groupedStream.count(sessionWindows, storeSupplier)
  //      .mapValues[Long](javaLong => Long.box(javaLong))
  //  }

  def reduce(reducer: (V, V) => V,
             storeName: String): KTableS[K, V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, storeName)
  }

  //  KTable[K, V] reduce( Reducer[V] reducer,
  //   StateStoreSupplier[KeyValueStore] storeSupplier)]
  //

  def reduce[W <: Window](reducer: (V, V) => V,
                          windows: Windows[W],
                          storeName: String): KTableS[Windowed[K], V] = {
    val reducerS: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerS, windows, storeName)
  }

  //  [W <: Window] KTable[Windowed[K], V] reduce( Reducer[V] reducer,
  //   Windows[W] windows,
  //   StateStoreSupplier[WindowStore] storeSupplier)]
  //

  def reduce(reducer: (V, V) => V,
             sessionWindows: SessionWindows,
             storeName: String): KTableS[Windowed[K], V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, sessionWindows, storeName)
  }

  //  KTable[Windowed[K], V] reduce( Reducer[V] reducer,
  //   SessionWindows sessionWindows,
  //   StateStoreSupplier[SessionStore] storeSupplier)]

  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    aggValueSerde: Serde[VR],
                    storeName: String): KTableS[K, VR] = {
    val initializerS: Initializer[VR] = () => initializer()
    val aggregatorS: Aggregator[K, V, VR] = (k: K, v: V, va: VR) => aggregator(k, v, va)
    inner.aggregate(initializerS, aggregatorS, aggValueSerde, storeName)
  }

  //  [VR] KTable[K, VR] aggregate( Initializer[VR] initializer,
  //     Aggregator[? super K, ? super V, VR] aggregator,
  //     StateStoreSupplier[KeyValueStore] storeSupplier)]

  def aggregate[W <: Window, VR](initializer: () => VR,
                                 aggregator: (K, V, VR) => VR,
                                 windows: Windows[W],
                                 aggValueSerde: Serde[VR],
                                 storeName: String): KTableS[Windowed[K], VR] = {
    val initializerS: Initializer[VR] = () => initializer()
    val aggregatorS: Aggregator[K, V, VR] = (k: K, v: V, va: VR) => aggregator(k, v, va)
    inner.aggregate(initializerS, aggregatorS, windows, aggValueSerde, storeName)
  }

  //  [W <: Window, VR] KTable[Windowed[K], VR] aggregate( Initializer[VR] initializer,
  //     Aggregator[? super K, ? super V, VR] aggregator,
  //     Windows[W] windows,
  //       StateStoreSupplier[WindowStore] storeSupplier)]
  //
  //  [T] KTable[Windowed[K], T] aggregate( Initializer[T] initializer,
  //     Aggregator[? super K, ? super V, T] aggregator,
  //     Merger[? super K, T] sessionMerger,
  //     SessionWindows sessionWindows,
  //     Serde[T] aggValueSerde,
  //       String storeName)]
  //
  //  [T] KTable[Windowed[K], T] aggregate( Initializer[T] initializer,
  //     Aggregator[? super K, ? super V, T] aggregator,
  //     Merger[? super K, T] sessionMerger,
  //     SessionWindows sessionWindows,
  //     Serde[T] aggValueSerde,
  //       StateStoreSupplier[SessionStore] storeSupplier)]

}
