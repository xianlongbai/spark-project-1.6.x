package com.spar_rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/25.
  *
  * 分区并查看各个分区中的数据
  */
object mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("mapPartitionsWithIndex")
    val sc = new SparkContext(conf)

    var rdd = sc.parallelize(List((1,1), (1,2), (2,3), (2,4), (3,5), (3,6),(4,7), (4,8),(5,9), (5,10)),3)

    def mapPartIndexFunc(i1:Int,iter: Iterator[(Int,Int)]):Iterator[(Int,(Int,Int))]={
      var res = List[(Int,(Int,Int))]()
      while(iter.hasNext){
        var next = iter.next()
        res=res.::(i1,next)
      }
      res.iterator
    }
    val mapPartIndexRDD = rdd.mapPartitionsWithIndex((i: Int, arr: Iterator[(Int, Int)]) =>{
      var res = List[(Int,(Int,Int))]()
      while (arr.hasNext){
        var next = arr.next()
        res = res.::(i,next)
      }
      res.iterator
    })

    mapPartIndexRDD.foreach(println( _))


  }
}
