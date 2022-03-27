package com.dahua.DIM

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ZoneDimForRDD {
  def main(args: Array[String]): Unit = {
    if (args != 2) {
      println(
        """
          |无参数
          |input path   outputpath
          |""".stripMargin)
      sys.exit()
    }

    //创建sparksession对象
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    //接受参数
    var Array(inputpath)=args


    val df: DataFrame = spark.read.parquet(inputpath)
    df.map(row=>{
      //获取字段
      val requestMode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")

    })


  }
}
