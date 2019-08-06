package com.spark.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by root on 2019/7/17.
  */
object Joindf {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df001: DataFrame = sqlContext.createDataFrame(Seq(
      (1,"ming", 20, 15552211521L),
      (2,"hong", 19, 13287994007L),
      (3,"zhi", 21, 15552211523L)
    ))toDF("id","name", "age", "phone")

    val df002: DataFrame = sqlContext.createDataFrame(Seq(
      (1,"男", 65, 1),
      (2,"女", 79, 2),
      (4,"男", 89, 3)
    ))toDF("id", "sex", "wight","pid")


    val df003: DataFrame = sqlContext.createDataFrame(Seq(
      (1,"男", 65, 1),
      (2,"女", 79, 2),
      (4,"男", 89, 3),
      (5,"男", 89, 3),
      (6,"男", 89, 3)
    ))toDF("id", "sex", "wight","pid")

    val res: DataFrame = df001.join(df002,Seq("id"),"right")
    res.show(false)
    val res2 = df001.join(df002,df001("id") === df002("pid"),"right")
    res2.show(false)

    val result: DataFrame = df003.groupBy("sex").agg("wight" -> "avg")
    result.show(false)

  }


}
