package com.tags

import com.myutils.TagsMake
import org.apache.spark.sql.Row

object eqipmentTags extends TagsMake {
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    //设备类型
    val client: Int = row.getAs[Int]("client")
    client match {
      case t if t==1 => list:+=("Android "+"D0001000"+t,1)
      case t if t==2 => list:+=("IOS "+"D0001000"+t,1)
      case t if t==3 => list:+=( "WinPhone"+"D0001000"+t,1)
      case _ =>  list:+= ("其他"+"D00010004" ,1)
    }
    //设备联网方式
    val networkmannername:String= row.getAs[String]("networkmannername")
    networkmannername match {
     case t if t.equals("Wifi")  => list:+=("Wifi"+" D00020001",1)
     case t if t.equals("4G") =>list:+=("4G"+" D00020002",1)
     case t if t.equals("3G") =>list:+=("3G"+" D00020003",1)
     case t if t.equals("2G") =>list:+=("2G"+" D00020004",1)
     case _ => list:+=("其他"+"D00020005",1)
   }
    //运营商名称
    val ispname: String = row.getAs[String]("ispname")
    ispname match {
      case t if t.equals("移动") => list:+=("移动"+" D00030001",1)
      case t if t.equals("联通") => list:+=("联通"+" D00030002",1)
      case t if t.equals("电信") => list:+=("电信"+" D00030003",1)
      case _ =>list:+=("其他"+"D0030004",1)
    }
    list
  }
}
