import com.github.retronym.SbtOneJar._

oneJarSettings

name := "metadata_clean_up"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
  "com.typesafe" % "config" % "1.3.1"
)