package com.spar_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by root on 2018/3/14.
  */
object doublepolymerization {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("doublepolymerization")
    val sc = new SparkContext(conf)

    val ordd: RDD[String] = sc.parallelize(Array("yyy","yyy","yyy","yyy","yyy","yyy","yyy","yyy"))
    ordd.map(x => {
      var r: Int = 1+Random.nextInt(2)
      (r+"_"+x,1)
    }).reduceByKey(_+_,2).map(x => {   //这里将stage的并行度提高为2
      (x._1.split("_")(1),x._2)
    }).reduceByKey(_+_,1).foreach(println)
    //while (true){}
  }

}
