package com.spark.spark_rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/14.
  */
class reparttionTest {

}
object reparttionTest{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val dataArr: Array[String] = Array("Tom01","Tom02","Tom03"
      ,"Tom04","Tom05","Tom06"
      ,"Tom07","Tom08","Tom09"
      ,"Tom10","Tom11","Tom12")

    sc.parallelize(dataArr).repartition(4).coalesce(1,false).coalesce(4,true).foreach(println)
    while (true){}


  }

}