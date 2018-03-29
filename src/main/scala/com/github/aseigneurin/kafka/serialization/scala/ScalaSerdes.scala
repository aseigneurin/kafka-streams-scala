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
