package com.exam

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
  */
object JSONParse {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()
    val srcDF: DataFrame = spark.read.json("data/json.json")
    //data/json.json
    import spark.implicits._
    srcDF.printSchema()

    srcDF.foreach(x=>{
      val regJSON: String = x.getAs[String]("regeocode")
      println(regJSON.toString)
    })
//    val a: Dataset[String] = srcDF.map(x => {
//      //val status: String = x.getAs[String]("status")
//
//      val regJSON: JSONObject = x.getAs[JSONObject]("regeocode")
//      //      val poisArray: JSONArray = regeocodeJSON.getJSONArray("pois")
//      //      val result = collection.mutable.ListBuffer[String]()
//      //      // 循环数组
//      //      for (item <- poisArray.toArray()) {
//      //        if (item.isInstanceOf[JSONObject]) {
//      //          val json = item.asInstanceOf[JSONObject]
//      //          val businessarea = json.getString("businessarea")
//      //          result.append(businessarea)
//      //        }
//      //      }
//      //      result.mkString(",")
//      regJSON.toString
//    })
//    a.show()
      //.filter(_._1.equals("1")).map(_._2).flatMap(_.split(",")).map((_, 1)).groupByKey(_).mapValues()
    ////        srcDF.printSchema()
    //        //1、按照pois，分类businessarea，并统计每个businessarea的总数。
    //        srcDF.map(x => {
    //          //      if (regeocodeJSON == null) return ""
    //          val poisArray: JSONArray = regeocodeJSON.getJSONArray("pois")
    //          val result = collection.mutable.ListBuffer[String]()
    //          // 循环数组
    //          for (item <- poisArray.toArray()) {
    //            if (item.isInstanceOf[JSONObject]) {
    //              val json = item.asInstanceOf[JSONObject]
    //              val businessarea = json.getString("businessarea")
    //              result.append(businessarea)
    //            }
    //          }
    //          result.mkString(",")
    //        }).flatMap(_.split(","))
    //          .map((_, 1))
    //          .reduceByKey(_ + _)
    //          .collect
    //          .toBuffer
    //        // 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
    //        srcDF.rdd.map(row => {
    //          val line: String = row.toString()
    //          val srcJSON: JSONObject = JSON.parseObject(line)
    //          val status: Int = srcJSON.getIntValue("status")
    //
    //          val regeocodeJSON: JSONObject = srcJSON.getJSONObject("regeocode")
    //
    //          val poisArray: JSONArray = regeocodeJSON.getJSONArray("pois")
    //          val result = collection.mutable.ListBuffer[String]()
    //          // 循环数组
    //          for (item <- poisArray.toArray()) {
    //            if (item.isInstanceOf[JSONObject]) {
    //              val json = item.asInstanceOf[JSONObject]
    //              val type1 = json.getString("type")
    //              result.append(type1)
    //            }
    //          }
    //          result.mkString(",")
    //        }).flatMap(_.split(","))
    //          .flatMap(_.split(";"))
    //          .map((_, 1))
    //          .reduceByKey(_ + _).collect.toBuffer
    //  }
  }
}
