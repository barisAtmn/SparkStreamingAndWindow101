package com.baris.spark101

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object ProgramStreamingLowLevel extends SparkHelper {

  /**
    * entry Point to the DStreams API
   **/
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    val transformation = socketStream.flatMap(line => line.split(" "))

    // action
    transformation.print()
    // or
    // transformation.saveAsTextFiles() // each folder are RDD, each file is a partition of RDD

    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }
}
