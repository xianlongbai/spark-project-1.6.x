package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by root on 2018/3/19.
  */
object udaf {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("udaf").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.json("people.txt")
    df.printSchema()
    df.registerTempTable("people")
    //创建udf 函数
    sqlContext.udf.register("strCount",new StringCount)

    sqlContext.sql("select strCount(age) from people").show()

  }



}
