package com.spark.spark_sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/15.
  */
class dfToJson {

}
object dfToJson{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val infos = Array("{'name':'zhangsan', 'age':55}","{'name':'lisi', 'age':30}","{'name':'wangwu', 'age':19}")
    val scores = Array("{'name':'zhangsan', 'score':155}","{'name':'lisi', 'score':130}")

    val infoRdd = sc.parallelize(infos)
    val scoreRdd = sc.parallelize(scores)

    val infoDF: DataFrame = sqlContext.read.json(infoRdd)
    val scoreDF = sqlContext.read.json(scoreRdd)

    infoDF.printSchema()
    scoreDF.printSchema()

    infoDF.registerTempTable("info")
    scoreDF.registerTempTable("score")

    val res: DataFrame = sqlContext.sql("select i.name,i.age,s.score from info i join score s on i.name = s.name")

    res.show(false)

    //存为hive中的一个表
    res.write.saveAsTable("student")



  }

}
