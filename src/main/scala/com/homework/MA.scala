package com.homework

import com.myutils.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 媒体分析
  */
object MA {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath, outputPath, dictPath) = args

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val srcDF: DataFrame = spark.read.parquet(inputPath)
    import spark.implicits._
    //"data/app_dict.txt"
    val DictDF: Dataset[String] = spark.read.textFile(dictPath)
    val dictMap: Map[String, String] = DictDF.map(x => x.split("\\s"))
      .filter(_.length >= 5)
      .map(arr => (arr(1), arr(4)))
      .collect
      .toMap
    //将字典广播出去
    val broadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(dictMap)
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
       //对媒体字典的url进行匹配，因为源文件中appname是空的，只能通过appid进行匹配
      val dictMap: Map[String, String] = broadcast.value
      //取每天相关字段
      var appName: String = row.getAs[String]("appname")
      //对空白appname进行处理
       if(StringUtils.isBlank(appName)){
         appName=dictMap.getOrElse(row.getAs[String]("appid"),"unknown")
       }
      (appName,reqPt++reqClick++adpt)
    })
      .reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2))
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)
  }
}
