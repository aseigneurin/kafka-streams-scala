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

object IntAsStringSerde extends BaseSerde[Int] {

  override def serialize(topic: String, data: Int): Array[Byte] = {
    data.toString.getBytes("UTF-8")
  }

  override def deserialize(topic: String, data: Array[Byte]): Int = data match {
    case null => 0
    case _ =>
      val str = new String(data, "UTF-8")
      str.toInt
  }

}

object LongAsStringSerde extends BaseSerde[Long] {

  override def serialize(topic: String, data: Long): Array[Byte] = {
    data.toString.getBytes("UTF-8")
  }

  override def deserialize(topic: String, data: Array[Byte]): Long = data match {
    case null => 0L
    case _ =>
      val str = new String(data, "UTF-8")
      str.toLong
  }

}

object DoubleAsStringSerde extends BaseSerde[Double] {

  override def serialize(topic: String, data: Double): Array[Byte] = {
    data.toString.getBytes("UTF-8")
  }

  override def deserialize(topic: String, data: Array[Byte]): Double = data match {
    case null => 0.0
    case _ =>
      val str = new String(data, "UTF-8")
      str.toDouble
  }

}
