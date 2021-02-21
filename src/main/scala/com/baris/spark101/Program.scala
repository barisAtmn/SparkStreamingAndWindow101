package com.baris.spark101

import org.apache.spark.sql.SparkSession

object Program {

  val spark =
    SparkSession.builder().master("local[8]").appName("Spark101").getOrCreate()

  import spark.implicits._

  val ds = spark.createDataset(Seq("Baris", "Ataman"))

  def main(args: Array[String]): Unit = {
    ds.explain(true)

  }

}
