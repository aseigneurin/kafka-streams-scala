package com.github.aseigneurin.kafka.serialization.scala

import org.apache.kafka.common.serialization.Serde

object ScalaSerdes {

  def serdeFrom[T](serialize: (String, T) => Array[Byte],
                   deserialize: (String, Array[Byte]) => T
                  ): Serde[T] = {
    new BaseSerde[T] {
      override def serialize(topic: String, data: T): Array[Byte] = serialize(topic, data)

      override def deserialize(topic: String, data: Array[Byte]): T = deserialize(topic, data)
    }
  }

}
