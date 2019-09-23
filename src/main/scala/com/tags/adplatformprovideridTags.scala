package com.tags

import com.myutils.TagsMake
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object adplatformprovideridTags extends TagsMake {
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
     if(StringUtils.isNotBlank(adplatformproviderid.toString)){
              list:+=("CN"+adplatformproviderid,1)
     }
    list
  }
}
