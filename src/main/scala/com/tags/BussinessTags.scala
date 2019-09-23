package com.tags

import ch.hsr.geohash.GeoHash
import com.myutils.{AmapUtil, AmapsUtil, JedisConnectionPool, String2Type, TagsMake}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object BussinessTags extends TagsMake {
  override def mkTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //获取数据
    val row: Row = args(0).asInstanceOf[Row]
    //获取经纬度
    if(String2Type.toDouble(row.getAs[String]("long1")) >=22
      && String2Type.toDouble(row.getAs[String]("long1")) <=136
      && String2Type.toDouble(row.getAs[String]("lat"))>=3
      && String2Type.toDouble(row.getAs[String]("lat"))<=53) {
      //经纬度
      val long1 = row.getAs[String]("long1").toDouble
      val lat = row.getAs[String]("lat").toDouble
      //获取商圈名称
      val business: String = getBusiness(long1, lat)
      if (StringUtils.isNotBlank(business)) {
        val strs: Array[String] = business.split(",",-1)

        strs.foreach(str => {
          list :+= (str, 1)
        })

      }
    }
    list
  }

  /**
    * 获取商圈信息
    *
    */
  def getBusiness(long1: Double, lat: Double): String = {
    //GeoHash码
    val geohash1: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long1 , 6)
    //数据库查询当前商圈信息
    val business: String = redis_queryBusiness(geohash1)
    //去高德请求
    if (business == null) {
      val business: String = AmapsUtil.getBusinessFromAmap(long1, lat)
      //将高德地图获取的商圈存入reds
      if (business.length>0) {
        redis_insertBussiness(geohash1, business)
      }
    }
    business
  }

  /**
    * 在数据库查找商圈信息
    *
    * @param geohash
    * @return
    */
  def redis_queryBusiness(geohash: String): String = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    val business: String = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 将从高德获取的商圈保存redis数据库
    */
  def redis_insertBussiness(geohash: String, business: String) = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
