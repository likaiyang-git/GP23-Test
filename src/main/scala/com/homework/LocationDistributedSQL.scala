package com.homework

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 地域分布指标
  */
object LocationDistributedSQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()
    val srcDF: DataFrame = spark.read.parquet("data/output")

    srcDF.createOrReplaceTempView("log")

    spark.sql(
      """
        |select
        |provincename,
        |cityname,
        |org_request,
        |effe_request,
        |ad_request,
        |bid_sum,
        |bid_success_sum,
        |ifnull(bid_success_sum/bid_sum,0),
        |show_sum,
        |click_sum ,
        |ad_consumer,
        |ad_cost
        |from(
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode=1 and  processnode>=1 then 1 else 0 end) org_request,
        |sum(case when requestmode=1 and  processnode>=2 then 1 else 0 end)  effe_request,
        |sum(case when requestmode=1 and  processnode>=3 then 1 else 0 end)   ad_request,
        |sum(case when iseffective=1 and isbilling=1 and ispid=1 then 1 else 0 end)   bid_sum,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end)    bid_success_sum,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) show_sum,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)  click_sum ,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end)  ad_consumer,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end)  ad_cost
        |from(
        |select
        |provincename,
        |cityname,
        |requestmode,
        |processnode,
        |iseffective,
        |isbilling,
        |ispid,
        |iswin,
        |adorderid,
        |winprice,
        |adpayment
        |from log) t  group by  provincename,cityname ) t1
        |union all
        |select
        |cityname,
        |provincename,
        |org_request,
        |effe_request,
        |ad_request,
        |bid_sum,
        |bid_success_sum,
        |ifnull(bid_success_sum/bid_sum,0),
        |show_sum,
        |click_sum ,
        |ad_consumer,
        |ad_cost
        |from(
        |select
        |provincename,
        |max(cityname) cityname,
        |sum(case when requestmode=1 and  processnode>=1 then 1 else 0 end) org_request,
        |sum(case when requestmode=1 and  processnode>=2 then 1 else 0 end)  effe_request,
        |sum(case when requestmode=1 and  processnode>=3 then 1 else 0 end)   ad_request,
        |sum(case when iseffective=1 and isbilling=1 and ispid=1 then 1 else 0 end)   bid_sum,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end)    bid_success_sum,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) show_sum,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)  click_sum ,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end)  ad_consumer,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end)  ad_cost
        |from(
        |select
        |provincename,
        |cityname,
        |requestmode,
        |processnode,
        |iseffective,
        |isbilling,
        |ispid,
        |iswin,
        |adorderid,
        |winprice,
        |adpayment
        |from log) t2  group by  provincename) t3
      """.stripMargin).show(1000)

  }
}
