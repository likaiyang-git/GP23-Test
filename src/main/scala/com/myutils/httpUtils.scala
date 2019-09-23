package com.myutils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils


/**
  * http请求协议 ，GET协议
  */
object httpUtils {

  //把url传给高德，高德返回一个JSON字符串
  def get(url: String): String = {
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //发送请求
    val resp: CloseableHttpResponse = client.execute(httpGet)
    //处理返回请求结果
    //处理乱码 ,从高德获取到的url中如果包含中文，则有可能乱码
    EntityUtils.toString(resp.getEntity,"UTF-8")

  }
}
