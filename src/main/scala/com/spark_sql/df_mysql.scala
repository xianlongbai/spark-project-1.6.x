package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by root on 2018/3/15.
  */
object df_mysql {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("df_json2")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val options : Map[String, String] =  Map("url" -> "jdbc:mysql://localhost:3306/spider",
                "driver" -> "com.mysql.jdbc.Driver", "user" -> "root","password" -> "root","dbtable" -> "big_goods")

    val sqldf: DataFrame = ssc.read.format("jdbc").options(options).load()
    sqldf.registerTempTable("goods")

    val options2: Map[String, String] = options.++(Map("dbtable" -> "goods_info"))
    val sqldf2: DataFrame = ssc.read.format("jdbc").options(options2).load()
    sqldf2.registerTempTable("goods2")

    ssc.sql("select * from goods limit 10").show()
    ssc.sql("select * from goods2 limit 10").show()

  }


}
