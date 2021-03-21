package com.baris.spark101

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import ru.chermenin.spark.sql.execution.streaming.state.implicits._

import java.sql.Timestamp

object SparkStreamingWatermarksWithRocksDB {

  val sparkWithRocksDB = SparkSession
    .builder()
    .appName("SparkStreaming")
    .master("local[8]")
    .config("spark.driver.maxResultSize", "8g") // Advanced select spark and paste spark.driver.maxResultSize 0 (for unlimited) or whatever the value suits you.
    .useRocksDBStateStore()
    .getOrCreate()

  val linesFromSocket: DataFrame = sparkWithRocksDB.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()

  import sparkWithRocksDB.implicits._

  /**
    *  A 2 seconds watermark means
    *   - a window will only be considered until the wm surpasses the window end
    *   -
   **/
  def testWatermarks() = {
    val dataDF = linesFromSocket
      .select(
        col("value").as("color"),
        current_timestamp().as("processingTime")
      )

    val watermarkedDF = dataDF
      .withWatermark("processingTime", "1 minute")
      .groupBy(
        window(col("processingTime"), "30 seconds").as("window"),
        col("color")
      )
      .count()
      .selectExpr("window", "color", "count")

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/sparkCheckPoint")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    println(sparkWithRocksDB.conf.get("spark.sql.streaming.checkpointLocation"))
    testWatermarks()
  }

}
