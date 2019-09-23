package com.tags

import com.myutils.TagsUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * 上下文的标签
  */
object TagsContext1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val srcDF: DataFrame = spark.read.parquet("data/output")

    //调用Hbase API
    //加载配置文件
    val load: Config = ConfigFactory.load()
    //获取表名
    val HbaseTableName: String = load.getString("HBASE.tableName")
    //创建Hadoop任务，配置hbase连接
    val configuration: Configuration = spark.sparkContext.hadoopConfiguration
    //配置Hbase连接
    configuration.set("hbase.zookeeper.quorum", load.getString("HBASE.Host"))
    //获取与hbase的connection连接
    val hbconf: Connection = ConnectionFactory.createConnection(configuration)
    //获取admin
    val hbadmin: Admin = hbconf.getAdmin
    //判断当前表是否被使用 ,如果没有的话就创建
    if (!hbadmin.tableExists(TableName.valueOf(HbaseTableName))) {
      println("当前表可用")
      // 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
      // 创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")
      // 将创建好的列簇加入表中
      tableDescriptor.addFamily(hColumnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconf.close()
    }
    //saveAsHadoopDataset的参数
    val conf = new JobConf(configuration)
    // 指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出哪张表
    conf.set(TableOutputFormat.OUTPUT_TABLE, HbaseTableName)


    val dictDS: Dataset[String] = spark.read.textFile("data/app_dict.txt")
    import spark.implicits._
    val dictMap: Map[String, String] = dictDS.map(x => x.split("\\s"))
      .filter(_.length >= 5)
      .map(arr => (arr(1), arr(4))).collect.toMap

    val broadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(dictMap)

    //读取停用信息
    val stopDS: Dataset[String] = spark.read.textFile("data/app_dict.txt")
    import spark.implicits._
    val stopMap: Map[String, Int] = stopDS.map((_, 0)).collect.toMap

    val broadcastStop: Broadcast[Map[String, Int]] = spark.sparkContext.broadcast(stopMap)
    //处理数据信息
    import spark.implicits._
    val allUserId: RDD[(List[String], Row)] = srcDF.rdd.map(row => {
      //获取所有id
      val strList: List[String] = TagsUtils.getAllUserId(row)
      (strList, row)
    })
    //1.让点和点有关系；
    //2.只让一个点携带标签

    //构建点的集合
    val verties: RDD[(Long, List[(String, Int)])] = allUserId.flatMap(row => {
      //获取所有数据
      val rows: Row = row._2
      //给广告位打标签
      val adtags: List[(String, Int)] = adTags.mkTags(rows)
      //  给APP打标签
      val appTags: List[(String, Int)] = AppTags.mkTags(rows, broadcast)
      //给渠道打标签
      val qudaoTags: List[(String, Int)] = adplatformprovideridTags.mkTags(rows)
      //给设备打标签
      val eqTags: List[(String, Int)] = eqipmentTags.mkTags(rows)
      //给关键字打标签
      val kwList = TagsKword.mkTags(rows, broadcastStop)
      //给地域打标签
      val areaTags: List[(String, Int)] = AreaTags.mkTags(rows)
      // 给商圈打标签
      val bussinessTags: List[(String, Int)] = BussinessTags.mkTags(rows)
      //获取所有的标签
      val tagList: List[(String, Int)] = adtags ++ appTags ++ qudaoTags ++ eqTags ++ kwList ++ areaTags ++ bussinessTags
      //保留用户id ,VD保存所有标签和id
      val VD: List[(String, Int)] = row._1.map((_, 0)) ++ tagList
      //思考：1.如何保证其中一个ID携带者用户的标签
      //      2.用户id的字符串如何处理
      row._1.map(uId => {
        if (row._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })


    })
//    verties.foreach(println)
    //构建边的集合
    val edges: RDD[Edge[Int]] = allUserId.flatMap(row => {
      row._1.map(uId => Edge(row._1.head.hashCode.toLong, uId.hashCode.toLong, 0))
    })
//    edges.foreach(println)

    //构建图
    val graph = Graph(verties,edges)
    //根据图计算中的连通图算法，通过图中的分支，连通所有的点
    //找到内部最小的点，为当前的公共点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

  //聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnid,tagsAndUserId)) =>{
        (cnid,tagsAndUserId)
      }
    } .reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }) .map{
      case (userId,userTags) =>{
        // 设置rowkey和列、列名
        val put = new Put(Bytes.toBytes(userId))      //设置rowkey
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(20190923),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(conf)

  }

}
