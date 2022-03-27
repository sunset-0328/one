package com.dahua.analyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalysework {
  def main(args: Array[String]): Unit = {

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").getOrCreate()
    var sc = spark.sparkContext

    val df: DataFrame = spark.read.parquet("hdfs://192.168.137.10:8020/output1")
    df.createTempView("log")

    //编写sql语句
    val sql="select provincename ,cityname, row_number() over(partition by provincename order by cityname) as pcount from log group by provincename,cityname"
    spark.sql(sql)
    spark.stop()
  }
}
