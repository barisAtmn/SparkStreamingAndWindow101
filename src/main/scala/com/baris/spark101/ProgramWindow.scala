package com.baris.spark101
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.util.chaining._

object ProgramWindow extends SparkHelper {

  import spark.implicits._

  val simpleData = Seq(
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")

  /**
    * row_number() window function is used to give the sequential row number starting from 1 to the result of each window
    * partition.
    *
   **/
  def rowNumber() = {

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("row_number", row_number.over(windowSpec))
      .show()
  }

  /**
    * RANK
    * The rows within a partition that have the same values will receive the same rank.
    *
   **/
  /**
    * rank() window function is used to provide a rank to the result within a window partition.
    * This function leaves gaps in rank when there are ties.
   **/
  def rankWindow() = {

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("rank", rank().over(windowSpec))
      .show()
  }

  /**
    * dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps.
    * This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
   **/
  def denseRankWindow() = {

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("dense_rank", dense_rank().over(windowSpec))
      .show()
  }

  def percentRankWindow() = {

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("percent_rank", percent_rank().over(windowSpec))
      .show()
  }

  /**
    * the LEAD function is an analytic function that lets you query more than one row in a table at a time without having to
    * join the table to itself. It returns values from the next row in the table.
    *
   **/
  def leadWindow() = {

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("lead", lead("salary", 2).over(windowSpec))
      .show()
  }

  /**
    * Lag() function to access previous rows data as per defined offset value.
   **/
  def lagWindow() = {
    val windowSpec = Window.partitionBy("department").orderBy("salary")

    df.withColumn("lag", lag("salary", 2).over(windowSpec))
      .show()
  }

  def aggWindow() = {

    val windowSpec = Window.partitionBy("department").orderBy("salary")

    val windowSpecAgg = Window.partitionBy("department")

    val aggDF = df
      .withColumn("row", row_number.over(windowSpec))
      .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("min", min(col("salary")).over(windowSpecAgg))
      .withColumn("max", max(col("salary")).over(windowSpecAgg))
      .where(col("row") === 1)
      .select("department", "avg", "sum", "min", "max")
      .show()

  }

  def main(args: Array[String]): Unit = {
    df.show
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    rowNumber()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    rankWindow()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    denseRankWindow()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    percentRankWindow()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    leadWindow()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    lagWindow()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
    aggWindow()
    ("-" * 100)
      .pipe(data => Console.YELLOW + data + Console.RESET)
      .tap(println)
  }

}
