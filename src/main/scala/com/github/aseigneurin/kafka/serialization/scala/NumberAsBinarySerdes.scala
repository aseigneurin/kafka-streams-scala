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

import org.apache.kafka.common.serialization._

object IntSerde extends BaseSerde[Int] {

  val innerSerializer = new IntegerSerializer
  val innerDeserializer = new IntegerDeserializer

  override def serialize(topic: String, data: Int): Array[Byte] = innerSerializer.serialize(topic, data)

  override def deserialize(topic: String, data: Array[Byte]): Int = innerDeserializer.deserialize(topic, data)

}

object LongSerde extends BaseSerde[Long] {

  val innerSerializer = new LongSerializer
  val innerDeserializer = new LongDeserializer

  override def serialize(topic: String, data: Long): Array[Byte] = innerSerializer.serialize(topic, data)

  override def deserialize(topic: String, data: Array[Byte]): Long = innerDeserializer.deserialize(topic, data)

}

object DoubleSerde extends BaseSerde[Double] {

  val innerSerializer = new DoubleSerializer
  val innerDeserializer = new DoubleDeserializer

  override def serialize(topic: String, data: Double): Array[Byte] = innerSerializer.serialize(topic, data)

  override def deserialize(topic: String, data: Array[Byte]): Double = innerDeserializer.deserialize(topic, data)

}
