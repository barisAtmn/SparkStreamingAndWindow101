import sbt._

object Dependencies {

  val sparkVersion = "3.0.1"

  lazy val loggingLibraries = Seq(
    "org.apache.logging.log4j" % "log4j" % "2.8.2" pomOnly (),
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val sparkLibraries = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion
  )

  lazy val rocksDB = Seq("ru.chermenin" %% "spark-states" % "0.2")

  lazy val delta = Seq("io.delta" %% "delta-core" % "0.8.0")

  lazy val s3 = Seq(
    "org.apache.hadoop" % "hadoop-aws" % "3.2.1",
    "com.amazonaws" % "aws-java-sdk" % "1.11.661" exclude ("com.fasterxml.jackson.core", "jackson-databind"),
    "org.apache.hadoop" % "hadoop-common" % "3.2.1",
    "joda-time" % "joda-time" % "2.10.5"
  )

}
