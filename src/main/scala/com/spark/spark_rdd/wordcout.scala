package com.spark.spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.{Lz4Codec, SnappyCodec, GzipCodec, BZip2Codec}

/**
  * Created by root on 2019/7/9.
  */
object wordcout {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)

    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    //如果使用rdd1.saveAsTextFile(“file:///tmp/lxw1234.com”)将文件保存到本地文件系统，那么只会保存在Executor所在机器的本地目录
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("D:\\tmp\\spark_rdd\\res_01")
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).foreach(println)
    //val array: Array[(String, Int)] = sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).collect()
    //val disRdd: RDD[String] = sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).map(_._1)
    //基于java序列化保存文件
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).saveAsObjectFile("D:\\tmp\\spark_rdd\\res_02")
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("D:\\tmp\\spark_rdd\\res_03",classOf[Lz4Codec])
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsSequenceFile("D:\\tmp\\spark_rdd\\res_04")
    // todo ...
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsHadoopFile()


  }

}
