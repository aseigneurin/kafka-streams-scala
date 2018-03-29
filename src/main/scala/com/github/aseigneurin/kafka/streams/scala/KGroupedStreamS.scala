/**
 * Copyright 2017-2018 Alexis Seigneurin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.github.aseigneurin.kafka.streams.scala

import com.github.aseigneurin.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.StateStoreSupplier
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}

class KGroupedStreamS[K, V](inner: KGroupedStream[K, V]) {

  def count(storeName: String): KTableS[K, Long] = {
    inner.count(storeName)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  def count(storeSupplier: StateStoreSupplier[KeyValueStore[_, _]]): KTableS[K, Long] = {
    inner.count(storeSupplier)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  def count[W <: Window](windows: Windows[W],
                         storeName: String): KTableS[Windowed[K], Long] = {
    inner.count[W](windows, storeName)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  def count[W <: Window](windows: Windows[W],
                         storeSupplier: StateStoreSupplier[WindowStore[_, _]]): KTableS[Windowed[K], Long] = {
    inner.count[W](windows, storeSupplier)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  def count(sessionWindows: SessionWindows, storeName: String): KTableS[Windowed[K], Long] = {
    inner.count(sessionWindows, storeName)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  def count(sessionWindows: SessionWindows,
            storeSupplier: StateStoreSupplier[SessionStore[_, _]]): KTableS[Windowed[K], Long] = {
    inner.count(sessionWindows, storeSupplier)
      .mapValues[Long](javaLong => Long.box(javaLong))
  }

  def reduce(reducer: (V, V) => V,
             storeName: String): KTableS[K, V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, storeName)
  }

  def reduce(reducer: (V, V) => V,
             storeSupplier: StateStoreSupplier[KeyValueStore[_, _]]): KTableS[K, V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, storeSupplier)
  }

  def reduce[W <: Window](reducer: (V, V) => V,
                          windows: Windows[W],
                          storeName: String): KTableS[Windowed[K], V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, windows, storeName)
  }

  def reduce[W <: Window](reducer: (V, V) => V,
                          windows: Windows[W],
                          storeSupplier: StateStoreSupplier[WindowStore[_, _]]): KTableS[Windowed[K], V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, windows, storeSupplier)
  }

  def reduce(reducer: (V, V) => V,
             sessionWindows: SessionWindows,
             storeName: String): KTableS[Windowed[K], V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, sessionWindows, storeName)
  }

  def reduce(reducer: (V, V) => V,
             sessionWindows: SessionWindows,
             storeSupplier: StateStoreSupplier[SessionStore[_, _]]): KTableS[Windowed[K], V] = {
    val reducerJ: Reducer[V] = (v1: V, v2: V) => reducer(v1, v2)
    inner.reduce(reducerJ, sessionWindows, storeSupplier)
  }

  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    aggValueSerde: Serde[VR],
                    storeName: String): KTableS[K, VR] = {
    val initializerJ: Initializer[VR] = () => initializer()
    val aggregatorJ: Aggregator[K, V, VR] = (k: K, v: V, va: VR) => aggregator(k, v, va)
    inner.aggregate(initializerJ, aggregatorJ, aggValueSerde, storeName)
  }

  def aggregate[VR](initializer: () => VR,
                    aggregator: (K, V, VR) => VR,
                    storeSupplier: StateStoreSupplier[KeyValueStore[_, _]]): KTableS[K, VR] = {
    val initializerJ: Initializer[VR] = () => initializer()
    val aggregatorJ: Aggregator[K, V, VR] = (k: K, v: V, va: VR) => aggregator(k, v, va)
    inner.aggregate(initializerJ, aggregatorJ, storeSupplier)
  }

  def aggregate[W <: Window, VR](initializer: () => VR,
                                 aggregator: (K, V, VR) => VR,
                                 windows: Windows[W],
                                 aggValueSerde: Serde[VR],
                                 storeName: String): KTableS[Windowed[K], VR] = {
    val initializerJ: Initializer[VR] = () => initializer()
    val aggregatorJ: Aggregator[K, V, VR] = (k: K, v: V, va: VR) => aggregator(k, v, va)
    inner.aggregate(initializerJ, aggregatorJ, windows, aggValueSerde, storeName)
  }

  def aggregate[W <: Window, VR](initializer: () => VR,
                                 aggregator: (K, V, VR) => VR,
                                 windows: Windows[W],
                                 storeSupplier: StateStoreSupplier[WindowStore[_, _]]): KTableS[Windowed[K], VR] = {
    val initializerJ: Initializer[VR] = () => initializer()
    val aggregatorJ: Aggregator[K, V, VR] = (k: K, v: V, va: VR) => aggregator(k, v, va)
    inner.aggregate(initializerJ, aggregatorJ, windows, storeSupplier)
  }

  def aggregate[T](initializer: Initializer[T],
                   aggregator: (K, V, T) => T,
                   sessionMerger: (K, T, T) => T,
                   sessionWindows: SessionWindows,
                   aggValueSerde: Serde[T],
                   storeName: String): KTableS[Windowed[K], T] = {
    val initializerJ: Initializer[T] = () => initializer()
    val aggregatorJ: Aggregator[K, V, T] = (k: K, v: V, t: T) => aggregator(k, v, t)
    val sessionMergerJ: Merger[K, T] = (aggKey: K, aggOne: T, aggTwo: T) => sessionMerger(aggKey, aggOne, aggTwo)
    inner.aggregate(initializerJ, aggregatorJ, sessionMergerJ, sessionWindows, aggValueSerde, storeName)
  }

  def aggregate[T](initializer: Initializer[T],
                   aggregator: (K, V, T) => T,
                   sessionMerger: Merger[_ >: K, T],
                   sessionWindows: SessionWindows,
                   aggValueSerde: Serde[T],
                   storeSupplier: StateStoreSupplier[SessionStore[_, _]]): KTableS[Windowed[K], T] = {
    val initializerJ: Initializer[T] = () => initializer()
    val aggregatorJ: Aggregator[K, V, T] = (k: K, v: V, t: T) => aggregator(k, v, t)
    val sessionMergerJ: Merger[K, T] = (aggKey: K, aggOne: T, aggTwo: T) => sessionMerger(aggKey, aggOne, aggTwo)
    inner.aggregate(initializerJ, aggregatorJ, sessionMergerJ, sessionWindows, aggValueSerde, storeSupplier)
  }

}
