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
