package com.etl


import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 测试类
  */
object test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("test")
      .getOrCreate()
    val srcDF: DataFrame = spark.read.parquet("data/output")
    import spark.implicits._
    srcDF.createOrReplaceTempView("log")
    spark.sql("select long1,lat from log").show(100)

  }
}
