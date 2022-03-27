package com.dahua.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalysework02 {
  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length != 1) {
      println("""
                |缺少参数
                |inputpath outputpath
                |""".stripMargin)
      sys.exit()
    }

    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._

    // 接收参数
    var Array(inputPath,outputPath) = args
    val line: RDD[String] = sc.textFile(inputPath)
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val proCityRDD: RDD[((String, String), Int)] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    })

    //降维
    val reduceRDD: RDD[((String, String), Int)] = proCityRDD.reduceByKey(_ + _)

    val rdd2: RDD[(String, (String, Int))] = reduceRDD.map(arr => {
      (arr._1._1, (arr._1._2, arr._2))
    })

     rdd2.map(x => {
      ((x._1, x._2._1), (x._2._2))
    }).reduceByKey(_ + _).count()



    spark.stop()
    sc.stop()
  }
}
