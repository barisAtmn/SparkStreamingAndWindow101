package com.baris

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

package object spark101 {

  /**
    *
  case class StructType(fields: Array[StructField])
      case class StructField(
            name: String,
            dataType: DataType,
            nullable: Boolean = true,
            metadata: Metadata = Metadata.empty
       )
   **/
  val stockSchema = StructType(
    Array(
      StructField("Date", DateType, true),
      StructField("Open", DoubleType, true),
      StructField("High", DoubleType, true),
      StructField("Low", DoubleType, true),
      StructField("Close", DoubleType, true),
      StructField("Volume", DoubleType, true),
      StructField("OpenInt", IntegerType, true)
    )
  )

  val purchaseSchema = StructType(
    Array(
      StructField("id", StringType, true),
      StructField("time", TimestampType, true),
      StructField("item", StringType, true),
      StructField("quantity", IntegerType, true)
    )
  )

  val spark = SparkSession
    .builder()
    .appName("SparkStreaming")
    .master("local[8]")
    .config("spark.driver.maxResultSize", "8g") // Advanced select spark and paste spark.driver.maxResultSize 0 (for unlimited) or whatever the value suits you.
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
    .getOrCreate()

  case class Person(name: Option[String], lastname: Option[String])

}
