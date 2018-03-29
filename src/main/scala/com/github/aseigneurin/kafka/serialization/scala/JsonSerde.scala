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
