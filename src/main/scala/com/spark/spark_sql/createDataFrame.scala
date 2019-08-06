package com.spark.spark_sql

import java.util

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by root on 2019/7/15.
  *
  * 如果直接用toDF()而不指定列名字，那么默认列名为"_1", "_2", ...
  *
  */

case class Person(name: String, age: Int)

object createDataFrame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //方式一:本地seq + toDF创建DataFrame
    val df001: DataFrame = sqlContext.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    ))toDF("name", "age", "phone")

    //方式二:通过case class + toDF创建DataFrame
    import sqlContext.implicits._
    val df002: DataFrame = sc.textFile("people_df").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//    df002.registerTempTable("people")
//    sqlContext.sql("select * from people where age > 25").show(false)

    //方式三：通过row+schema创建
    val schema: StructType = StructType(List(
      StructField("integer_column", IntegerType, nullable = false),
      StructField("string_column", StringType, nullable = true),
      StructField("date_column", DateType, nullable = true)
    ))

    val rdd: RDD[Row] = sc.parallelize(Seq(
      Row(1, "First Value", java.sql.Date.valueOf("2010-01-01")),
      Row(2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
    ))
    val df003: DataFrame = sqlContext.createDataFrame(rdd, schema)
    df003.registerTempTable("table")
    //df003.show()

    //方式四：通过文件直接创建DataFrame
//    val df004 = sqlContext.read.parquet("path...")
//    val df005 = sqlContext.read.json("path...")

    //方式五：使用csv文件,spark2.0+之后的版本可用
//    val df006 = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true") //reading the headers
//      .option("mode", "DROPMALFORMED")
//      .load("csv/file/path"); //.csv("csv/file/path") //spark 2.0 api




//    val schema2 = StructType(List(
//      StructField("name", StringType, true),
//      StructField("age", IntegerType, true),
//      StructField("phone", LongType, true)
//    ))

      val schema2 = StructType(
        StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) ::
        StructField("phone", LongType, true) ::  Nil
      )

    val dataList = new util.ArrayList[Row]()
    dataList.add(Row("ming",20,15552211521L))
    dataList.add(Row("hong",19,13287994007L))
    dataList.add(Row("zhi",21,15552211523L))
    val df007 = sqlContext.createDataFrame(dataList,schema2)
    df007.registerTempTable("info")
    df007.show()


  }

}
