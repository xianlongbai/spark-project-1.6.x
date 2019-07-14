package com.spark.spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/14.
  */
class sampleTest {

}

object sampleTest{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)

    //抽样
    val arrayRdd: RDD[String] = sc.makeRDD(Array(
      "hello1","hello2","hello3","hello4","hello5","hello6",
      "world1","world2","world3","world4"
    ))
    arrayRdd.sample(false,0.3).foreach(println)



  }

}
