package com.spark.spark_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by root on 2019/7/14.
  */
class groupTopN {

}

object groupTopN{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val groupRDD: RDD[(String, Iterable[String])] = sc.textFile("score.txt").map(x =>(x.split("\t")(0),x.split("\t")(1))).groupByKey()

    val res: RDD[(String, Seq[Int])] = groupRDD.map((tuple: (String, Iterable[String])) =>{
      val tmp: Seq[Int] = tuple._2.toList.map(_.toInt)
      val r: Seq[Int] = tmp.sortBy((-_)).take(3)
      (tuple._1,r)
    })

    res.foreach(println)


  }

}
