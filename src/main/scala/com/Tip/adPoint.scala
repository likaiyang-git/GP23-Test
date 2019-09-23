package com.Tip

import org.apache.spark.sql.{DataFrame, SparkSession}

object adPoint {
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
    srcDF.rdd.map(row=>{
      val userid: String = row.getAs[String]("userid")
      var adspacetype= row.getAs[Int]("adspacetype").toString
      if(adspacetype.toInt<10){
           adspacetype match {
             case _ => adspacetype="LC"+"0"+adspacetype+"->"
           }
      }else{
          adspacetype match {
            case _=> adspacetype="LC"+adspacetype+"->"
          }
      }

      ((userid,adspacetype),1)

         }).reduceByKey(_+_).map(x=>x._1._1+x._1._2+x._2).saveAsTextFile(outputPath)
  }
}
