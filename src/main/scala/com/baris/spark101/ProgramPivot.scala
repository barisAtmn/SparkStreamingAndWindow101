package com.baris.spark101
import scala.util.chaining._
import org.apache.spark.sql.functions._

/**
  * Pivot === Rotate
  * Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset
  * column into multiple columns (transform rows to columns) and unpivot is used to transform it back
  * (transform columns to rows).
  * */
object ProgramPivot extends SparkHelper {

  val data = Seq(
    ("Banana", 1000, "USA"),
    ("Carrots", 1500, "USA"),
    ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"),
    ("Orange", 2000, "USA"),
    ("Banana", 400, "China"),
    ("Carrots", 1200, "China"),
    ("Beans", 1500, "China"),
    ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"),
    ("Carrots", 2000, "Canada"),
    ("Beans", 2000, "Mexico")
  )
  import spark.sqlContext.implicits._
  val df = data.toDF("Product", "Amount", "Country")

  /**
    * Spark SQL provides pivot() function to rotate the data from one column into multiple columns
    * (transpose rows into columns). It is an aggregation where one of the grouping columns values transposed
    * into individual columns with distinct data.
   **/
  def pivotDF() = {
    df.groupBy("Product").pivot("Country").sum("Amount")
  }

  def unpivotDF() = {
    pivotDF()
      .select(
        $"Product",
        expr(
          "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
        )
      )
      .where("Total is not null")
  }

  def main(args: Array[String]): Unit = {
    df.show()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    pivotDF().show()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    unpivotDF().show()
  }

}
