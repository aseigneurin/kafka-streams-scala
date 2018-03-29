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

import java.util.{Locale, Properties}

import com.github.aseigneurin.kafka.serialization.scala.LongAsStringSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

// copied and adapted from https://github.com/apache/kafka/blob/trunk/streams/examples/src/main/java/org/apache/kafka/streams/examples/wordcount/WordCountDemo.java
object WordCountDemo {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    // Note: To re-run the demo, you need to use the offset reset tool:
    // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    implicit val stringSerde = Serdes.String
    implicit val longSerde = LongAsStringSerde

    val source = KStreamBuilderS.stream[String, String]("streams-file-input")

    val counts: KTableS[String, Long] = source
      .flatMapValues { value => value.toLowerCase(Locale.getDefault).split(" ") }
      .map { (_, value) => (value, value) }
      .groupByKey
      .count("Counts")

    counts.to("streams-wordcount-output")

    val streams = new KafkaStreams(KStreamBuilderS.inner, props)
    streams.start()

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    Thread.sleep(5000L)

    streams.close()
  }

}
