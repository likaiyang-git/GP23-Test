package com.Tip

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object appName {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("输入目录不正确")
    }
    val Array(inputPath, outputPath, dictPath) = args

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val srcDF: DataFrame = spark.read.parquet(inputPath)
    val dictDF = spark.read.textFile(dictPath)
    val dictDF2: Map[String, String] = dictDF.map(x => {
      x.split("\\s", -1)

    }).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect()
      .toMap
    val broadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(dictDF2)

    srcDF.rdd.map(row => {
      var appname: String = row.getAs[String]("appname")
      val dict: Map[String, String] = broadcast.value
      if (StringUtils.isBlank(appname)) {
        appname = dict.getOrElse(row.getAs[String]("appid"), "known")
      }
      appname match {
        case _ => appname="APP"+appname+"->"
      }
      (appname,1)
    }).reduceByKey(_+_).map(x=>x._1+x._2).saveAsTextFile(outputPath)
  }
}
