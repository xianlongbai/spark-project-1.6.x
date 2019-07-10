package com.spar_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2018/3/12.
  */
class mapPartition {}
object mapPartition{


  def demo: Unit ={


    //可变集合
    val map = new mutable.HashMap[String, Int]()
    val arrayBuffer: mutable.Seq[Int] = new mutable.ArrayBuffer[Int]()
    val buf: ListBuffer[Int] = new ListBuffer[Int]
    var que = scala.collection.mutable.Queue[String]()
    var stack = scala.collection.mutable.Stack[Int]()
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("wcjob")
    val sc = new SparkContext(conf)

    val array = Array("aaaa","bbbb","cccc","ddddd","eeee","fffff","pppp")

    val lineRddd: RDD[String] = sc.makeRDD(array)
    val cacheRdd = lineRddd.persist()
    lineRddd.repartition(1).map((str: String) =>{
      println("start~")
      println("输出~")
      println("end~")
      str
    }).foreach(println)

    val tmp: RDD[String] = cacheRdd.repartition(1).mapPartitions((str: Iterator[String]) =>{
      val list = new ListBuffer[String]()
      val list2: Seq[String] = List("1","2","3","4")
      val list3: Seq[Int] = Seq(1,2,3,4)
      val y: Seq[Int] = Range(1, 5)
      println("start~")
      while (str.hasNext) {
        list += str.next() + "!!!"
      }
      println("end~")
      list.iterator
    })

    tmp.foreach(println)
  }

}