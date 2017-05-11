name := "kafka-streams-scala"

organization := "com.github.aseigneurin"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.2"

scalacOptions := Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.0"

