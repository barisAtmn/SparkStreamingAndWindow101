package com.baris.spark101
import com.baris.spark101.ProgramStreaming.linesFromSocket
import org.apache.spark.sql.functions._

object ProgramStreamingWindow extends SparkHelper {

  def readPurchasesFromSocket() = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), purchaseSchema).as("purchase"))
      .selectExpr("purchase.*")
  }

  def aggPurchasesBySlidingWindow() = {
    val purchaseDF = readPurchasesFromSocket()

    /**
      * We have 24 different window per day as windowDuration is 1 day and slideDuration is 1 hour!!!
      *
     **/
    val windowByDay =
      purchaseDF
        .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start,end}
        .agg(sum("quantity").as("totalQuantity"))

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def bestSellingProductPerDay() = {
    val purchaseDF = readPurchasesFromSocket()

    /**
      * We have 24 different window per day as windowDuration is 1 day!!!
      *
     **/
    val bestSellingProduct =
      purchaseDF
        .groupBy(window(col("time"), "1 day").as("time"), col("id")) // struct column: has fields {start,end}
        .agg(sum("quantity").as("totalQuantity"))
        .sort("totalQuantity", "id")

    bestSellingProduct.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def bestSellingProductPerDay2() = {
    val purchaseDF = readPurchasesFromSocket()

    /**
      * We have 24 different window per day as windowDuration is 1 day and slideDuration!!!
      *
     **/
    val bestSellingProduct =
      purchaseDF
        .groupBy(window(col("time"), "1 day").as("day"), col("item")) // struct column: has fields {start,end}
        .agg(sum("quantity").as("totalQuantity"))
        .orderBy(col("day"), col("totalQuantity").desc)

    bestSellingProduct.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  /**
    * Use to take processing time ==> current_timestamp
   **/
  def aggByProcessingTime() = {
    val df = linesFromSocket
      .select(col("value"), current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"), "10 seconds").as("windows"))
      .agg(sum(length(col("value"))))

    df.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  /**
    * nc -lk 12345
  {"id":"049e385a-7bda-409d-a438-be4facb02af5","time":"2019-03-02T05:38:27.675+02:00","item":"Watch","quantity":2}
  {"id":"48e794df-429e-4605-ad96-8d4b3104b798","time":"2019-03-03T15:31:28.675+02:00","item":"MacBook","quantity":6}
  {"id":"4e5760a3-90ea-4bc2-b9c6-a1683298d80e","time":"2019-03-03T15:45:11.675+02:00","item":"TV","quantity":5}
  {"id":"4e5760a3-90ea-4bc2-b9c6-a1683298d80e","time":"2019-03-03T16:45:11.675+02:00","item":"TV","quantity":5}
   **/
  def main(args: Array[String]): Unit = {
    // aggPurchasesBySlidingWindow()
    // bestSellingProductPerDay()
    // bestSellingProductPerDay2()
    aggByProcessingTime()
  }

}
