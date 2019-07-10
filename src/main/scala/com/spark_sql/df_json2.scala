package com.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/15.
  */
object df_json2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("df_json2")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val infos = Array("{'name':'zhangsan', 'age':55}","{'name':'lisi', 'age':30}","{'name':'wangwu', 'age':19}")
    val scores = Array("{'name':'zhangsan', 'score':155}","{'name':'lisi', 'score':130}")

    val ageRdd = sc.parallelize(infos)
    val scoreRdd: RDD[String] = sc.parallelize(scores)

    val ageDf: DataFrame = ssc.read.json(ageRdd)
    val scoreDf: DataFrame = ssc.read.json(scoreRdd)

    //sc.setCheckpointDir("hdfs://...")

    ageDf.registerTempTable("info")
    scoreDf.registerTempTable("score")

    ssc.sql("select i.name,i.age,s.score from info i left join score s on i.name = s.name").show()
  }

}
