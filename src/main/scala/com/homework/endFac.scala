package com.homework

import com.myutils.RptUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object endFac {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入目录不正确")
    }
    val Array(inputPath, outputPath) = args

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val srcDF: DataFrame = spark.read.parquet(inputPath)
    import spark.implicits._
    srcDF.rdd.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val ispid: Int = row.getAs[Int]("ispid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val reqPt: List[Double] = RptUtils.ReqPt(requestmode, processnode)
      val reqClick: List[Double] = RptUtils.ReqClick(requestmode, iseffective)
      val adpt: List[Double] = RptUtils.adpt(iseffective, isbilling, ispid, iswin, adorderid, winprice, adpayment)
      (row.getAs[String]("ispname"),reqPt++reqClick++adpt)
    }).reduceByKey((x,y)=>x.zip(y).map(x=>x._2+x._1)).saveAsTextFile(outputPath)
                   

  }
}
