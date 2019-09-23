package com.myutils

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object mediumDict {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val DictDF: Dataset[String] = spark.read.textFile("data/app_dict.txt")
    import spark.implicits._
    DictDF.map(x => x.split("\\s"))
      .filter(_.length >= 5)
      .map(arr => (arr(1), arr(4))).collect.toMap

  }
}
