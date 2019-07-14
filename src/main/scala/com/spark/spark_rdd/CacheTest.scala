package com.spark.spark_rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/14.
  */
class CacheTest {

}
object CacheTest{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val dataArr: Array[String] = Array("Tom01","Tom02","Tom03"
      ,"Tom04","Tom05","Tom06"
      ,"Tom07","Tom08","Tom09"
      ,"Tom10","Tom11","Tom12")

    val x: Broadcast[String] = sc.broadcast("|")
    val initRdd = sc.parallelize(dataArr)
//    initRdd.cache()
    initRdd.persist(StorageLevel.MEMORY_ONLY)

    initRdd.map(_+x.value).foreach(println)





  }
}