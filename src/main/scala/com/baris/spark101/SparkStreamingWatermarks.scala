package com.baris.spark101

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

object SparkStreamingWatermarks extends SparkHelper {

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = {
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString
        println(s"$i - $queryTime")
      }
    }).start()
  }

  /**
    *  A 2 seconds watermark means
    *   - a window will only be considered until the wm surpasses the window end
    *   -
   **/
  def testWatermarks() = {
    val dataDF = linesFromSocket
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)
        (timestamp, data)
      }
      .toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds")
      .groupBy(window(col("created"), "2 seconds"))
      .count()
      .selectExpr("window.*", "color", "count")

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .option("truncate", false)
      .start()

    debugQuery(query)

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermarks()
  }

}
