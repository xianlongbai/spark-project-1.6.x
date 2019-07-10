package com.spar_rdd

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.util.Sorting

/**
  * Created by root on 2018/3/25.
  */
object repartitionAndSortWithinPartitions2 {

  def main(args: Array[String]): Unit = {


    //设置master为local，用来进行本地调试
    val conf = new SparkConf().setAppName("Student_partition_sort").setMaster("local")
    val sc = new SparkContext(conf)

    val nameList = Array((1, "xuruyun"),
      (2, "liangyongqi"),
      (5, "liangyongqi"),
      (6, "liangyongqi"),
      (4, "liangyongqi"),
      (3, "wangfei"));



    val nameRDD = sc.parallelize(nameList)
    val relRdd = nameRDD.repartitionAndSortWithinPartitions(new Partitioner {

      override def getPartition(key: Any): Int = key.hashCode() % numPartitions

      override def numPartitions: Int = 2
    })

    relRdd.foreach(println)

  }

}
