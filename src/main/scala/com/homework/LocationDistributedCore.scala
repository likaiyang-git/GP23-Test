package com.homework

import com.myutils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 地域指标
  */
object LocationDistributedCore {
  def main(args: Array[String]): Unit = {
//    if (args.length != 2) {
//      println("目录不正确")
//    }
//    val Array(inputPath, outputPath) = args
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val srcDF: DataFrame = spark.read.parquet("data/outputaa")
    import spark.implicits._
    val rseDF: RDD[((String, String), List[Double])] = srcDF.rdd.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val ispid: Int = row.getAs[Int]("ispid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      //处理请求数
      val ptList: List[Double] = RptUtils.ReqPt(requestmode, processnode)
      //处理展示和点击
      val cliskList: List[Double] = RptUtils.ReqClick(requestmode, iseffective)
      //处理广告
      val adlist: List[Double] = RptUtils.adpt(iseffective, isbilling, ispid, iswin
        , adorderid, winprice, adpayment)
      val allList: List[Double] = ptList ++ cliskList ++ adlist
      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), allList)
    }).reduceByKey((x, y) => {
      x.zip(y).map(t => t._1 + t._2)
    })
    rseDF.map(t=>t._1+","+t._2.mkString(",")).saveAsTextFile("data/output_pt")

  }
}
