package com.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/11.
  */
object groupTopN {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("groupTopN")
    val sc = new SparkContext(conf)

    val groupRDD: RDD[(String, Iterable[String])] = sc.textFile("score.txt").map(x =>(x.split("\t")(0),x.split("\t")(1))).groupByKey()
//    val topNRdd: RDD[List[Int]] = groupRDD.map((tuple: (String, Iterable[String])) =>{
//      var t = List[Int]()
//      for (x <- tuple._2){
//        t = t.::(x.toInt)
//      }
//      t.sortBy((-_)).take(3)
//    })
//    topNRdd.foreach(println)
    groupRDD.map((tuple: (String, Iterable[String])) =>{
      val tmp: Seq[Int] = tuple._2.toList.map(_.toInt)
      val r: Seq[Int] = tmp.sortBy((-_)).take(3)
      (tuple._1,r)
    }).foreach(println)

  }
}
