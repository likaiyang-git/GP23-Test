package com.tags

import com.myutils.TagsMake
import org.apache.spark.sql.Row

object KeyTags extends TagsMake{
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val keywords: String = row.getAs[String]("keywords")
    val arr: Array[String] = keywords.split("\\|")
    if(arr.length>=3 && arr.length<8) {
      for(i <- 0 until arr.length){
        list:+=("K"+arr(i),1)
      }
    }
    
    list
  }
}
