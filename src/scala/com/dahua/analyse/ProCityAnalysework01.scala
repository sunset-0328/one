package com.dahua.analyse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalysework01 {
  def main(args: Array[String]): Unit = {

    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).master("local[1]").appName("Log2Parquet").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._

    //传入参数
    var Array(inputPath, outputPath) = args
    val rdd: RDD[String] = sc.textFile(inputPath)
    val field: RDD[Array[String]] = rdd.map(_.split(",", -1))
    val proCityRDD: RDD[((String, String), Int)] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    })
    val rdd1: RDD[((String, String), Int)] = proCityRDD.map(x => {
      (x._1, x._2)
    })
    rdd1.reduceByKey(_+_).sortBy(_._2).saveAsTextFile(outputPath)

    spark.stop()
    sc.stop()
  }
}
