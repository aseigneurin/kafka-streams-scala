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
 
package com.github.aseigneurin.kafka.serialization.scala

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

abstract class BaseSerde[T] extends Serde[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serializer() = BaseSerializer(serialize)

  override def deserializer() = BaseDeserializer(deserialize)

  def serialize(topic: String, data: T): Array[Byte]

  def deserialize(topic: String, data: Array[Byte]): T

}

abstract class BaseSerializer[T] extends Serializer[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

}

object BaseSerializer {

  def apply[T](func: (String, T) => Array[Byte]) = new BaseSerializer[T] {
    override def serialize(topic: String, data: T): Array[Byte] = func(topic, data)
  }

}

abstract class BaseDeserializer[T] extends Deserializer[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

}

object BaseDeserializer {

  def apply[T](func: (String, Array[Byte]) => T) = new BaseDeserializer[T] {
    override def deserialize(topic: String, data: Array[Byte]): T = func(topic, data)
  }

}
