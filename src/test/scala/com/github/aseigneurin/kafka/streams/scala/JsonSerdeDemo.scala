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

import java.util.Properties

import com.github.aseigneurin.kafka.serialization.scala.JsonSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
  * Prerequesites:
  *   $ kafka-topics --zookeeper localhost:2181 --create --partitions 4 --replication-factor 1 --topic names
  *   $ kafka-topics --zookeeper localhost:2181 --create --partitions 4 --replication-factor 1 --topic users
  *   $ kafka-topics --zookeeper localhost:2181 --create --partitions 4 --replication-factor 1 --topic names-from-users
  *
  * Launch this code.
  *
  * Launch consumers:
  *   $ kafka-console-consumer --bootstrap-server localhost:9092 --topic users
  *   $ kafka-console-consumer --bootstrap-server localhost:9092 --topic names-from-users
  *
  * Launch a producer and type a few things:
  *   $ kafka-console-producer --broker-list localhost:9092 --topic names
  */

object JsonSerdeDemo {

  case class User(name: String)

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "names")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    implicit val stringSerde = Serdes.String
    implicit val userSerde = new JsonSerde[User]

    KStreamBuilderS.stream[String, String]("names")
      .mapValues { name => User(name) }
      .to("users")

    KStreamBuilderS.stream[String, User]("users")
      .mapValues { user => user.name }
      .to("names-from-users")

    val streams = new KafkaStreams(KStreamBuilderS.inner, props)
    streams.start()
  }

}
