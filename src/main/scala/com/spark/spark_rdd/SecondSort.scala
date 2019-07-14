package com.spark.spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/14.
  */
object SecondSort {

  def main(args: Array[String]): Unit = {
    val sconf=new SparkConf().setAppName("SecondSort").setMaster("local")
    val sc=new SparkContext(sconf)

    val lines = sc.textFile("secondSort.txt")

    val pairs: RDD[(SecondSortKey1, String)] = lines.map ( x=>(new SecondSortKey1(x.split(" ")(0).toInt,x.split(" ")(1).toInt),x) )
    val sortedPairs: RDD[(SecondSortKey1, String)] = pairs.sortByKey(false)
    //      val sortedPairs = pairs.sortBy(_._1, false)
    sortedPairs.map(_._2).foreach {println }

  }

}
class SecondSortKey1(val first:Int,val second:Int) extends Ordered[SecondSortKey1] with Serializable {
  def compare(that: SecondSortKey1): Int = {
    if(this.first-that.first==0)
      this.second- that.second
    else
      this.first-that.first
  }
}