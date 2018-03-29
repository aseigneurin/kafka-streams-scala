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
 
name := "kafka-streams-scala"

organization := "com.github.aseigneurin"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.11"

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

// Kafka
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.0"
// JSON
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.8"
// logging
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))
