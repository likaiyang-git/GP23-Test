package com.tags

import com.myutils.TagsMake
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 给广告打标签
  */
object adTags extends TagsMake{
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    //对传进来的参数进行强制类型转换
    val row: Row = args(0).asInstanceOf[Row]
    val adType: Int = row.getAs[Int]("adspacetype")
    val adName: String = row.getAs[String]("adspacetypename")
    // 广告位类型标签
    adType match {
      case  t if t>9 => list:+=("LC"+t,1)
      case t if t>0 && t<=9=>  list:+=("LC0"+t,1)
    }
    // 广告位名称标签
     if(StringUtils.isNotBlank(adName)){
       list:+=("LN"+adName,1)
     }
    list
  }
}
