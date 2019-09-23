package com.tags

import com.myutils.TagsMake
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTags extends TagsMake {
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val broad = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val appid: String = row.getAs[String]("appid")
    val appname: String = row.getAs[String]("appname")
    if(StringUtils.isNotBlank(appname)){
         list:+=("APP"+appname,1)
    } else{
      list:+=("APP"+broad.value.getOrElse(appid,"未知"),1)
    }
    list
  }
}
