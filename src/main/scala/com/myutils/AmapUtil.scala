package com.myutils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {
  /**
    * 获取经纬度
    *
    * @param long1
    * @param lat
    * @return
    */
  def getBusinessFromAmap(long1: Double, lat: Double): String = {
    //https://restapi.amap.com/v3/geocode/regeo?output=xml&location=116.310003,39.991957
    // &key=b85be88a6ca1cdf030b8c709bf212f1f&radius=1000&extensions=all

    //对经纬度进行拼接
    val location = long1 + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?output=xml&location=" + location + "&key=b85be88a6ca1cdf030b8c709bf212f1f&radius=1000&extensions=all"
    //输入url，返回JSON字符串
    val JSONStr: String = httpUtils.get(url)

    //解析JSON字符串，获取商圈信息
    val JSONObject: JSONObject = JSON.parseObject(JSONStr)
    //判断当前状态是否为1
    val responseJSON: JSONObject = JSONObject.getJSONObject("response")
    if(responseJSON ==null) return ""
    val statusCode: Int = responseJSON.getIntValue("status")
    if (statusCode == 0) return ""
    //如果不为空
    val regeocodeJSON: JSONObject = responseJSON.getJSONObject("regeocode")
    //判断regeocodeJSON是否为空
    if (regeocodeJSON == null) return ""
    val addressComponentJSON: JSONObject = regeocodeJSON.getJSONObject("addressComponent")
    if (addressComponentJSON == null) return ""
    val businessAreas: JSONArray = addressComponentJSON.getJSONArray("businessAreas")
     if(businessAreas==null) return ""
      //循环数组
    //定义集合取值
    val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    for(i<- businessAreas.toArray){
           if(i.isInstanceOf[JSONObject]){
             val json: JSONObject = i.asInstanceOf[JSONObject]
             val name: String = json.getString("name")
             result.append(name)
           }
    }
    //商圈名字
   result.mkString(",")
  }
}
