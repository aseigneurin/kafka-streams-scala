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
