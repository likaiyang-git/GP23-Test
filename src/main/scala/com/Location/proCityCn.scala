package com.Location

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object proCityCn {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("输入目录有误")
      sys.exit()
    }
    val Array(inputPath) = args
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val df1: DataFrame = spark.read.parquet(inputPath)

    //生成临时表
    df1.createOrReplaceTempView("log")

    val df2: DataFrame = spark.sql("select provincename,cityname,count(1) cn from log group by provincename,cityname")
    //输出json格式的文件，并且按照省，城市分区
    //df2.write.partitionBy("provincename", "cityname").json("e://json_output")
    //df2.coalesce(1).write.json("data/json_output2")

    // 通过config配置文件依赖进行加载相关的配置信息
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", load.getString("jdbc.user"))
    prop.setProperty("password", load.getString("jdbc.password"))
    df2.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"), load.getString("jdbc.tableName"), prop)
    spark.stop()
  }
}
