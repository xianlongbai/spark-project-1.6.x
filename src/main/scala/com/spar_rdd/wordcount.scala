package com.RDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/11.
  *spark-submit 参数
  * local 本地单线程
  * local[K] 本地多线程（指定K个内核）
  * local[*] 本地多线程（指定所有可用内核）
  * spark://HOST:PORT  连接到指定的 Spark standalone cluster master，需要指定端口。
  * mesos://HOST:PORT  连接到指定的  Mesos 集群，需要指定端口。
  * yarn-client客户端模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
  * yarn-cluster集群模式 连接到 YARN 集群
  */
//class wordcount {}

object wordcount{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc_001").setMaster("local")
    val sc = new SparkContext(conf)
//    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
//    sc.textFile("/user/root/bxl/wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("/user/root/spark_rel/")
    sc.textFile("wc.txt",3).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
  }


}
