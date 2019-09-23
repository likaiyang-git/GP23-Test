package com.etl

import com.myutils.String2Type
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将日志文件转换成parquet类型的文件
  */
object Text2Parquet {
  def main(args: Array[String]): Unit = {
    //    if(args.length !=2){
    //      println("目录不正确，退出程序")
    //      sys.exit()
    //    }

    //获取目录参数
    // val Array(inputPath,outputPath)=args


    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[2]")
      //采用KryoSerializer方式序列化结点之间传输的数据
      //设置序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //parquet 文件采用 Snappy 压缩方式
      .set("spark.io.compression.codec", "Snappy")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val srcRDD: RDD[String] = sc.textFile("data/2016-10-01_06_p1_invalid.1475274123982.log")

    val mapRDD: RDD[LogInfo] = srcRDD.map(t =>
      //若干位都是切割符，split(" ")不会继续切分，而split(" ", -1)会继续切分
      t.split(",", -1))     //0,,,,,,,,,
      .filter(y => y.length >= 85)
      .map(arr => {
      LogInfo(arr(0),
        String2Type.toInt(arr(1)),
        String2Type.toInt(arr(2)),
        String2Type.toInt(arr(3)),
        String2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        String2Type.toInt(arr(7)),
        String2Type.toInt(arr(8)),
        String2Type.toDouble(arr(9)),
        String2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        String2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        String2Type.toInt(arr(20)),
        String2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        String2Type.toInt(arr(26)),
        arr(27),
        String2Type.toInt(arr(28)),
        arr(29),
        String2Type.toInt(arr(30)),
        String2Type.toInt(arr(31)),
        String2Type.toInt(arr(32)),
        arr(33),
        String2Type.toInt(arr(34)),
        String2Type.toInt(arr(35)),
        String2Type.toInt(arr(36)),
        arr(37),
        String2Type.toInt(arr(38)),
        String2Type.toInt(arr(39)),
        String2Type.toDouble(arr(40)),
        String2Type.toDouble(arr(41)),
        String2Type.toInt(arr(42)),
        arr(43),
        String2Type.toDouble(arr(44)),
        String2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        String2Type.toInt(arr(57)),
        String2Type.toDouble(arr(58)),
        String2Type.toInt(arr(59)),
        String2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        String2Type.toInt(arr(73)),
        String2Type.toDouble(arr(74)),
        String2Type.toDouble(arr(75)),
        String2Type.toDouble(arr(76)),
        String2Type.toDouble(arr(77)),
        String2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        String2Type.toInt(arr(84)))
    })
//       srcRDD.map(x=>{
//         val arr: Array[String] = x.split(",",-1)
//         arr.length>=85
//         LogInfo(arr)
//       })


    import spark.implicits._
    val df: DataFrame = mapRDD.toDF()
    df.write.parquet("data/output")
    df.write.json("data/json_output")
    sc.stop()
    spark.stop()
  }
}

case class LogInfo(sessionid: String,
                   advertisersid: Int,
                   adorderid: Int,
                   adcreativeid: Int,
                   adplatformproviderid: Int,
                   sdkversion: String,
                   adplatformkey: String,
                   putinmodeltype: Int,
                   requestmode: Int,
                   adprice: Double,
                   adppprice: Double,
                   requestdate: String,
                   ip: String,
                   appid: String,
                   appname: String,
                   uuid: String,
                   device: String,
                   client: Int,
                   osversion: String,
                   density: String,
                   pw: Int,
                   ph: Int,
                   long1: String,
                   lat: String,
                   provincename: String,
                   cityname: String,
                   ispid: Int,
                   ispname: String,
                   networkmannerid: Int,
                   networkmannername: String,
                   iseffective: Int,
                   isbilling: Int,
                   adspacetype: Int,
                   adspacetypename: String,
                   devicetype: Int,
                   processnode: Int,
                   apptype: Int,
                   district: String,
                   paymode: Int,
                   isbid: Int,
                   bidprice: Double,
                   winprice: Double,
                   iswin: Int,
                   cur: String,
                   rate: Double,
                   cnywinprice: Double,
                   imei: String,
                   mac: String,
                   idfa: String,
                   openudid: String,
                   androidid: String,
                   rtbprovince: String,
                   rtbcity: String,
                   rtbdistrict: String,
                   rtbstreet: String,
                   storeurl: String,
                   realip: String,
                   isqualityapp: Int,
                   bidfloor: Double,
                   aw: Int,
                   ah: Int,
                   imeimd5: String,
                   macmd5: String,
                   idfamd5: String,
                   openudidmd5: String,
                   androididmd5: String,
                   imeisha1: String,
                   macsha1: String,
                   idfasha1: String,
                   openudidsha1: String,
                   androididsha1: String,
                   uuidunknow: String,
                   userid: String,
                   iptype: Int,
                   initbidprice: Double,
                   adpayment: Double,
                   agentrate: Double,
                   lomarkrate: Double,
                   adxrate: Double,
                   title: String,
                   keywords: String,
                   tagid: String,
                   callbackdate: String,
                   channelid: String,
                   mediatype: Int
                  )