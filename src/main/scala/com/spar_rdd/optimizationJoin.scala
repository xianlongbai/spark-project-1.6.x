package com.spar_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

/**
  * Created by root on 2018/3/14.
  */
object optimizationJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("optimizationJoin")
    val sc = new SparkContext(conf)

    val sum1 = sc.accumulator(1)
    val sum2 = sc.accumulator(2)

    val array1 = Array((1,"bxl"),(2,"wsy"),(3,"lxd"))
    val array2 = Array((1,86),(1,90),(1,88),(1,68),(2,99),(3,79))

    val ordd1 = sc.parallelize(array1)
    val ordd2 = sc.parallelize(array2)
    ordd1.persist()
    ordd2.persist()
    val result0 = ordd1.join(ordd2)
    result0.foreach(println)
//    val left_01: RDD[(Int, String)] = ordd1.filter(_._1==1)
//    val left_02 = ordd1.filter(_._1!=1)
//    val right_01 = ordd2.filter(_._1==1)
//    val right_02 = ordd2.filter(_._1!=1)
//    val change_01: RDD[(String, String)] = left_01.flatMap((tuple: (Int, String)) =>{
//      //var ll = new ListBuffer[]
//      val arr = new ArrayBuffer[Tuple2[String,String]]()
//      for (x <- 1 to 2){
//        //膨胀rdd有问题 todo...
//        var r = 1+Random.nextInt(2)
//        arr.append((r+"_"+tuple._1,tuple._2))
//      }
//      arr.iterator
//    })
//
//    val change_02: RDD[(String, Int)] = right_01.map(x => {
//      var r = 1+Random.nextInt(2)
//      (r+"_"+x._1,x._2)
//    })
//    val res_01: RDD[(String, (String, Int))] = left_02.join(right_02).map(x => (x._1.toString,x._2))
//    println("res_01的分区数："+res_01.partitions.size)
//    val res_02: RDD[(String, (String, Int))] = change_01.join(change_02,2).map(x => (x._1.split("_")(1),x._2))
//    println("res_02的分区数："+res_02.partitions.size)
//    val result: RDD[(String, (String, Int))] = res_01.union(res_02)
//    println("result的分区数："+result.partitions.size)
//    result.foreach(println)
    while (true){}
  }

}
