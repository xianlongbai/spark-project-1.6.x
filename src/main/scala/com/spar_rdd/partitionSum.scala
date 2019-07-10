package com.spar_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2018/3/13.
  *
  * 注意：在使用累加器的时候，如果需要统计总量值，必须将数据collect到driver端进行
  * 如果不进行collect,则各自的executer有各自的计算值
  * 2、必须执行active类算子才会更行累加器的值
  *
  *
  */
object partitionSum {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("partitionSum").setMaster("local")
    val sc = new SparkContext(conf)
    val count = sc.accumulator(0, "Error Accumulator")
    val count2 = sc.accumulator(0, "Error Accumulator")
    val ordd: RDD[Int] = sc.makeRDD(1 to 10,2)
    println("------>"+count.value)
    val rel = ordd.mapPartitionsWithIndex((i: Int, ints: Iterator[Int]) =>{
//      val buf = new ListBuffer[String]()
      var result = List[String]()
      var sum = 0;
      count.add(2)
      while (ints.hasNext){
        count2.add(1)
        sum+=ints.next()
      }
      result .:: (i+"@"+sum).iterator
    }).collect()
    rel.foreach(x =>{
      println(x)
      println(count)
      println(count2)
    })

  }

}
