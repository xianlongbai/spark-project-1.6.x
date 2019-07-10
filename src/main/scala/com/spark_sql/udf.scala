package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by root on 2018/3/19.
  *
  *
  */
object udf {

  //sqlContext.udf.register("strLen", (s: String) => s.length())

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("udf").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.json("people.txt")
    df.registerTempTable("people")
    //创建udf 函数
    sqlContext.udf.register("strLen",(s: String) => s.length())

    sqlContext.sql("select name, strLen(name) from people").show()


  }

}
