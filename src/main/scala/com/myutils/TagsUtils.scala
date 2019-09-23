package com.myutils

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {


  //获取用户唯一的id，把匹配到的第一个非空的字段当作用户的id
  def getOneUserId(row: Row) = {

    row match {
      case t if StringUtils.isNotBlank(t.getAs[String]("imei")) => "IM" + t.getAs[String]("imei")
      case t if StringUtils.isNotBlank(t.getAs[String]("mac")) => "MAC" + t.getAs[String]("mac")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfa")) => "IDFA" + t.getAs[String]("idfa")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudid"))=>"OpenUDID"+t.getAs[String]("openudid")
      case t if StringUtils.isNotBlank(t.getAs[String]("androidid")) =>"AndroidId" +t.getAs[String]("androidid")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5")) => "IMmd5" + t.getAs[String]("imeimd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("macmd5")) => "MACmd5" + t.getAs[String]("macmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5")) => "IDFAmd5" + t.getAs[String]("idfamd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5"))=>"OpenUDIDmd5"+t.getAs[String]("openudidmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5")) =>"AndroidIdmd5" +t.getAs[String]("androididmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1")) => "IMsha1" + t.getAs[String]("imeisha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("macsha1")) => "MACsha1" + t.getAs[String]("macsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1")) => "IDFAsha1" + t.getAs[String]("idfasha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1"))=>"OpenUDIDsha1"+t.getAs[String]("openudidsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1")) =>"AndroidIdsha1" +t.getAs[String]("androididsha1")
      case _ =>"其他"
    }
  }
  //获取所有id
  //用id充当一个点，相关的id之间构建一条边
  def   getAllUserId(row:Row):List[String]={
    var list=List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei")))   list:+="IM" + row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac")))   list:+="MAC" + row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa")))  list:+="IDFA" + row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid")) )    list:+="OpenUDID"+row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid")))     list:+="AndroidId" +row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5")))   list:+="IM" + row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5")))   list:+="MAC" + row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5")))  list:+="IDFA" + row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5")) )    list:+="OpenUDID"+row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5")))     list:+="AndroidId" +row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1")))   list:+="IM" + row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1")))   list:+="MAC" + row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1")))  list:+="IDFA" + row.getAs[String]("idfasha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1")) )    list:+="OpenUDID"+row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1")))     list:+="AndroidId" +row.getAs[String]("androididsha1")
    list
  }
}
