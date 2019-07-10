package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by root on 2018/3/15.
  */
object df_parquet {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("df_json2")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val df = ssc.read.format("parquet").load("hdfs://node1:8020/user/hive_external/merge_parquet/000000_0")
    df.registerTempTable("merge")
    df.printSchema()
    ssc.sql("select * from merge limit 10").show()

  }

}
