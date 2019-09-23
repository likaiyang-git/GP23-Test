package com.GraphTest

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例
  */
object graph_Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()

    //创建点和边
    val poiRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
      //后边那个元组代表第i个点的基本信息
      (1L, ("凯凯", 25)),
      (2L, ("小黑", 18)),
      (6L, ("小红", 13)),
      (9L, ("小蓝", 133)),
      (16L, ("小白", 55)),
      (133L, ("小彩", 23)),
      (138L, ("小牛", 11)),
      (44L, ("小猪", 17)),
      (21L, ("小错", 14)),
      (5L, ("小手", 11)),
      (7L, ("小李", 88)),
      (158L, ("马丁", 66))
    ))
    //构造边的集合
    val edgeRDD: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
      //初始的点，到的点，点的属性
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 133L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    //有点有边构建图
    val graph = Graph(poiRDD, edgeRDD)

    //取出每个边上的数值最小顶点（唯一顶点）
    val vertices: VertexRDD[VertexId] = graph
      .connectedComponents()
      .vertices

    /**
      * (158,5)
      * (138,1)
      * (6,1)
      * (44,1)
      */
    vertices.foreach(println)
    //汽配数据
    //最大顶点和每个点中的值进行join,可以得到每个顶点对应的所有的值的集合
    //(5,5).(5L, ("小手", 11))   =>  (5,(5,(小手,11)))
    //把有关联的数据放在一块

    vertices.join(poiRDD).map{
       case(userId,(conId,(name,age)))=>{
         (conId,List(name,age))
       }
    }.reduceByKey(_++_).foreach(println)
    //输出结果：
    //(1,List(小白, 55, 小牛, 11, 小红, 13, 小猪, 17, 小黑, 18, 小错, 14, 小彩, 23, 凯凯, 25, 小蓝, 133))
    //(5,List(马丁, 66, 小李, 88, 小手, 11))
  }
}
