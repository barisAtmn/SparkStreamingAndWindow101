import Dependencies._

name := "SparkStreamingAndWindow101"

version := "0.1"

scalaVersion := "2.12.12"

resolvers ++= Seq(
  "Central Repository" at "https://repo.maven.apache.org/maven2",
  "typesafe" at "https://repo.typesafe.com/typesafe/repo/"
)

libraryDependencies ++= sparkLibraries
libraryDependencies ++= rocksDB
libraryDependencies ++= loggingLibraries
libraryDependencies ++= delta
libraryDependencies ++= s3
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0"
libraryDependencies += "com.github.jnr" % "jnr-posix" % "3.1.4"
libraryDependencies += "com.github.bigwheel" %% "util-backports" % "2.1"

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties"
)
outputStrategy := Some(StdoutOutput)
