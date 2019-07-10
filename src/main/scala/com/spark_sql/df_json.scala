package com.spark_sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/15.
  * 请注意，作为JSON文件提供的文件不是典型的JSON文件。每一行必须包含一个独立的、自包含的有效JSON对象。
  * 因此，一个常规的多行JSON文件通常会失败。
  *
  */
object df_json {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dfJson").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    //这里支持json\parquet\jdbc\orc\table...
    val df = ssc.read.json("people.txt")
//    val df: DataFrame = ssc.read.format("json").load("people.txt")
    df.registerTempTable("people")
    df.show()
    df.printSchema()
    //show(int,boolean) 第一个参数显示多少行，第二个参数是字段显示多少个字符，默认20行，20个字符
    ssc.sql("select * from people where age > 20").show(true)


  }
}
