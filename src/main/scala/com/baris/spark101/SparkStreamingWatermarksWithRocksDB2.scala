package com.baris.spark101

import com.baris.spark101.SparkStreamingWatermarksWithRocksDB.linesFromSocket
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.chermenin.spark.sql.execution.streaming.state.implicits._

object SparkStreamingWatermarksWithRocksDB2 {

  val sparkWithRocksDB = SparkSession
    .builder()
    .appName("SparkStreaming")
    .master("local[8]")
    .config("spark.driver.maxResultSize", "8g") // Advanced select spark and paste spark.driver.maxResultSize 0 (for unlimited) or whatever the value suits you.
    .useRocksDBStateStore()
    .getOrCreate()

  /**
    *  A 2 seconds watermark means
    *   - a window will only be considered until the wm surpasses the window end
    *   -
   **/
  def testWatermarks() = {
    val dataDF = linesFromSocket
      .select(col("value").as("word"))

    val query = dataDF
      .dropDuplicates("word")
      .writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/sparkCheckPoint")
      .queryName("dropDuplicates")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testWatermarks()
  }

}
