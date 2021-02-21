package com.baris.spark101
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Encoders}
import scala.concurrent.duration.DurationInt

object ProgramStreaming extends SparkHelper {

  import spark.implicits._

  /**
    *  Read from socket and create DF
    *  then put it to console
   **/
  def readFromSocket() = {
    // read DF
    // Extract
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformations
    // Transform
    val shortLines: DataFrame = lines.filter(length($"value") <= 5)

    // check if it is static or streaming
    println(shortLines.isStreaming)

    // consume a DF
    // Load
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start() // Action for streaming

    // wait for stream to finish
    query.awaitTermination()

  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      //.option("dateFormat", "YYYY-MM-DD")
      .schema(stockSchema)
      .load("src/main/resources/data/Stocks/a*")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    // Extract
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformations
    // Transform
    val shortLines: DataFrame = lines.filter(length($"value") <= 5)

    /** consume a DF
       Load
       Trigger.Once() // single batch, then terminate
       Trigger.Continuous(2.seconds)
      */
    shortLines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) // every 2 seconds extract works!!!
      .start() // Action for streaming
      .awaitTermination()
  }

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount = lines.selectExpr("count(*) as lineCount")

    /**
      * append and update not supported on aggregations without watermark
      * aggregations with distinct are not supported, otherwise Spark will need to keep track of Everything
     **/
    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def numericalAggregations(aggFunction: Column => Column) = {

    val numbers = linesFromSocket.select($"value".cast("integer").as("numbers"))
    val aggDF = numbers.select(aggFunction($"numbers")).as("agg_so_far")

    aggDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * As complete mode is used, all data will be printed to console each time.
   **/
  def groupNames() = {

    val names = linesFromSocket
      .select($"value".as("name"))
      .groupBy($"name") // returns RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  /**
    * if you want to read JSON from console
    * .select(from_json(col("value"),schema)
    *
    *  Joins happen per batch!
   **/
  def joinStreamWithStatic() = {

    linesFromSocket
      .join(
        staticPlayersDF,
        linesFromSocket.col("value").cast(IntegerType) === staticPlayersDF.col(
          "id"
        )
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  /**
    * stream to stream => only append supported !!!
    * left/right outer joins are supported, but must have watermarks !!!
   **/
  def joinStreamWithStream = {
    val linesFromSocket2: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()

    linesFromSocket
      .join(
        linesFromSocket2,
        linesFromSocket.col("value") === linesFromSocket2.col("value")
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Streams as Dataset
   **/
  def readStreamAsDataset() = {
    val personEncoder = Encoders.product[Person]

    linesFromSocket
      .select(split(col("value"), ",").as("arr"))
      .select('arr.getItem(0).as("name"), 'arr.getItem(1).as("lastname"))
      .as[Person]
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // readFromSocket()
    // readFromFiles()
    // streamingCount()
    // numericalAggregations(sum)
    // groupNames()
    // joinStreamWithStatic()
    // joinStreamWithStream
    readStreamAsDataset()
  }

}
