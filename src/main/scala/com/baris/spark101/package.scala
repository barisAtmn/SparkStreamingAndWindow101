package com.baris

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  IntegerType,
  StructField,
  StructType
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

  val spark = SparkSession
    .builder()
    .appName("SparkStreaming")
    .master("local[8]")
    .config("spark.driver.maxResultSize", "8g") // Advanced select spark and paste spark.driver.maxResultSize 0 (for unlimited) or whatever the value suits you.
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  case class Person(name: Option[String], lastname: Option[String])

}
