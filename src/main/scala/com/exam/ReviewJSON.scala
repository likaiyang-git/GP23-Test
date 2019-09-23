package com.exam

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 status=0 失败）：
  */
object ReviewJSON {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()

    val srcDS: Dataset[String] = spark.read.textFile("data/json.txt")

    val arr: mutable.Buffer[String] = srcDS.rdd.collect.toBuffer
    val arr1: ArrayBuffer[String] = ArrayBuffer[String]()
    for(item <- arr){
      val srcObject: JSONObject = JSON.parseObject(item)
      if(srcObject==null) return ""

      val statusCode: Int = srcObject.getIntValue("status")
      if(statusCode==0) return ""

      val regeocodeObject: JSONObject = srcObject.getJSONObject("regeocode")
      if(regeocodeObject==null) return ""

      val poisArr: JSONArray = regeocodeObject.getJSONArray("pois")
      if(poisArr==null) return ""

      //声明一个可变的集合，用来保存businessarea
      val list: ListBuffer[String] = mutable.ListBuffer[String]()

      for(item <- poisArr.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val businessarea: String = json.getString("businessarea")
          list.append(businessarea)
        }
      }
      arr1.append(list.mkString(","))
    }
     arr1.flatMap(_.split(",")).filter(x=>x!="[]").map((_,1)).groupBy(_._1).mapValues(_.size).foreach(println)


     //2.
     val arr2: ArrayBuffer[String] = ArrayBuffer[String]()
    for(item <- arr) {
      val srcObject: JSONObject = JSON.parseObject(item)
      if (srcObject == null) return ""

      val statusCode: Int = srcObject.getIntValue("status")
      if (statusCode == 0) return ""

      val regeocodeObject: JSONObject = srcObject.getJSONObject("regeocode")
      if (regeocodeObject == null) return ""

      val poisArr: JSONArray = regeocodeObject.getJSONArray("pois")
      if (poisArr == null) return ""

      val list1: ListBuffer[String] = mutable.ListBuffer[String]()
      for(item<-poisArr.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val type1: String = json.getString("type")
          list1.append(type1)
        }
      }
      arr2.append(list1.mkString(","))
    }
     arr2.flatMap(_.split(",")).flatMap(_.split(";")).map((_,1)).groupBy(_._1).mapValues(_.size).foreach(println)

     import spark.implicits._
    val arr3: ArrayBuffer[String] = ArrayBuffer[String]()
    srcDS.rdd.map(x=>{
         val src: JSONObject = JSON.parseObject(x)
      val statusCode: Int = src.getIntValue("status")
//      if (statusCode == 0) return ""

      val regeocodeObject: JSONObject = src.getJSONObject("regeocode")
//      if (regeocodeObject == null) return ""

      val poisArr: JSONArray = regeocodeObject.getJSONArray("pois")
      if (poisArr == null) return ""

      val list1: ListBuffer[String] = mutable.ListBuffer[String]()
      for(item<-poisArr.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json: JSONObject = item.asInstanceOf[JSONObject]
          val type1: String = json.getString("type")
          list1.append(type1)
        }
      }
      arr3.append(list1.mkString(","))
    })
    arr3.flatMap(_.split(",")).flatMap(_.split(";")).map((_,1)).groupBy(_._1).mapValues(_.size).foreach(println)
  }
}
