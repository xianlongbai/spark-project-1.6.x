package com.spar_rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by root on 2018/3/14.
  * 问题：
  * 虽然只在map里进行了累加器加1的操作，但是两次得到的累加器的值却不一样，这是由于count和reduce都是action类型的操作，
  * 触发了两次作业的提交，所以map算子实际上被执行了了两次，在reduce操作提交作业后累加器又完成了一轮计数，所以最终累加器的值为18
  * 解决：
  *  在执行第一个action类算子时，就将rdd进行cache或者persist
  */
object accumulator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wcjob")
    val sc = new SparkContext(conf)
    val accum: Accumulator[Int] = sc.accumulator(0)
    val broad: Broadcast[String] = sc.broadcast("num")
    val oRdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2)
    val numberRDD = oRdd.map(n=>{
      accum.add(1)
      println("000000000000000000000000")
      n+1
    })
    numberRDD.persist().count
    println("accum1:"+accum.value)
    numberRDD.reduce(_+_)
    println("accum2: "+accum.value)
    println("-----------------------------------")
    //清除rdd缓存,基于内存和磁盘的缓存数据
    numberRDD.unpersist(true)
    numberRDD.map(broad.value+_).foreach(println)

  }
}
