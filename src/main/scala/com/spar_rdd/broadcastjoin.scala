package com.spar_rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by root on 2018/3/22.
  *
  *
  * 使用广播变量来模拟join
  */

object broadcastjoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("broadcastjoin").setMaster("local")
    val sc = new SparkContext(conf)

    val array1 = Array((1,"白先龙"),(2,"戴沐白"),(3,"唐三"),(4,"朱竹清"))
    val array2 = Array((1,100),(2,99))

    val ordd1 = sc.parallelize(array1)
    //如果此时的ordd2占用内存很小,小于executor memory * (90%*60%)
    //可以使用广播变量来模拟join
    val ordd2 = sc.parallelize(array2)

    val col: Array[(Int, Int)] = ordd2.collect()
    val map = scala.collection.mutable.Map[Int,Int]()
    for (x <- col){
      map += (x._1->x._2)
    }
    val broad: Broadcast[mutable.Map[Int, Int]] = sc.broadcast(map)

    val res: RDD[(Int, (String, Int))] = ordd1.map((tuple: (Int, String)) =>{
      val s: mutable.Map[Int, Int] = broad.value
      println(s)
      val id = tuple._1
      //val score = s.get(tuple._1)
      val score: Int = s.getOrElse(tuple._1,110)
      val name = tuple._2
      (id,(name,score))
    })

    res.collect().foreach(println)
  }

}
