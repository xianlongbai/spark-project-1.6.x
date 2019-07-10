package com.spar_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/14.
  */
object specialOperator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("specialOperator")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(1,2,3,3,4,5,5));
    val rdd2 = sc.parallelize(Array(1,2,3,3,4));
    //val parallelize = sc.parallelize(Arrays.asList(c));
    //返回在当前Rdd出现但不在另一个rdd出现的所有元素，且不去重
    val subtractRdd: RDD[Int] = rdd1.subtract(rdd2)
    subtractRdd.foreach(println)
    println("----------------------------------------------")
    //返回两个Rdd的交集,并去重
    val intersectionRdd = rdd1.intersection(rdd2)
    intersectionRdd.foreach(println)
    println("----------------------------------------------")
    //返回两个Rdd的 笛卡儿计算的结果
    val cartesianRdd = rdd1.cartesian(rdd2)
    cartesianRdd.foreach(println)
    println("----------------------------------------------")
    //返回两个Rdd的交集，不去重
    val unionRdd = rdd1.union(rdd2)
    unionRdd.foreach(println)
  }

}
