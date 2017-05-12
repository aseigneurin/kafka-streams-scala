package com.github.aseigneurin.kafka.serialization.scala

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.{ClassTag, classTag}

class JsonSerde[T >: Null : ClassTag] extends BaseSerde[T] with LazyLogging {

  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  override def deserialize(topic: String, data: Array[Byte]): T = data match {
    case null => null
    case _ =>
      try {
        mapper.readValue(data, classTag[T].runtimeClass.asInstanceOf[Class[T]])
      } catch {
        case e: Exception =>
          val jsonStr = new String(data, "UTF-8")
          logger.warn(s"Failed parsing ${jsonStr}", e)
          null
      }
  }

  override def serialize(topic: String, obj: T): Array[Byte] = {
    mapper.writeValueAsBytes(obj)
  }

}
