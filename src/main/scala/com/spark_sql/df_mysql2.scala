package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Created by root on 2018/3/15.
  */
object df_mysql2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("df_json2")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val dfReader : DataFrameReader = ssc.read.format("jdbc")
    dfReader.option("url", "jdbc:mysql://localhost:3306/spider")
    dfReader.option("driver", "com.mysql.jdbc.Driver")
    dfReader.option("user", "root")
    dfReader.option("password", "root")

    dfReader.option("dbtable", "big_goods")
    val df1: DataFrame = dfReader.load()
    dfReader.option("dbtable", "goods_info")
    val df2: DataFrame = dfReader.load()

    df1.registerTempTable("goods1")
    df2.registerTempTable("goods2")

    ssc.sql("select * from goods1 limit 10").show()
    ssc.sql("select * from goods2 limit 10").show()
  }
}
