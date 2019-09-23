package com.tags

import com.myutils.TagsMake
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AreaTags extends TagsMake {
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val provincename: String = row.getAs[String]("provincename")
    val cityname: String = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }
    if(StringUtils.isNoneEmpty(cityname)){
      list:+=("ZC"+cityname,1)
    }
    list
  }
}
