package com.baris.spark101

import org.apache.spark.sql.DataFrame

trait SparkHelper {

  val linesFromSocket: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load() // a DF with a single column "value" of type String

  val staticPlayersDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data.Players/*")
}
