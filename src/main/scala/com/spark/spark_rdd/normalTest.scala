package com.spark.spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2019/7/14.
  */
class normalTest {


}

object normalTest{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val dataArr: Array[String] = Array("Tom01","Tom02","Tom03"
      ,"Tom04","Tom05","Tom06"
      ,"Tom07","Tom08","Tom09"
      ,"Tom10","Tom11","Tom12")

    val list: Seq[(String, Int)] = List(
      ("bjsxt",1),
      ("bjsxt",1),
      ("bjsxt",2),
      ("shsxt",1)
    )

    val rdd1 = sc.makeRDD(Array(
      ("A",1),
      ("A",3),
      ("A",4),
      ("A",5),
      ("A",2),
      ("B",1),
      ("B",2),
      ("C",1)
    ),2)

    val rdd = sc.parallelize(1 to 10,2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()
    glomRDD.foreach(x => {
      x.foreach(println)
    })
    println(glomRDD.count())

    //val txtRdd = sc.makeRDD(list)

//    txtRdd.distinct().foreach(println)
//    println("------------------------------")
//    txtRdd.countByKey().foreach(println)
//    println("------------------------------")


//    rdd1.mapPartitionsWithIndex((index,iterator)=>{
//      val list = new ListBuffer[Tuple2[String,Int]]()
//      while (iterator.hasNext) {
//        val log = iterator.next()
//        list += log
//      }
//      list.iterator
//    }).count()

//    rdd1.combineByKey(
//      (v:Int)=>v+"_", (c:String,v:Int) => {c + "@" + v} , (c1:String,c2:String) => c1+"$"+c2, 4)
//      .collect().foreach(println)
//
//    rdd1.foreach(println)
//    println("------------")

    /*type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    val combineRDD: RDD[(String, (Int, Double))] = rdd1.combineByKey(
      (num:Int) => (1,num),
      (c1:MVType,newNum:Int) => (c1._1+1,c1._2+newNum),
      (c1:MVType,c2:MVType) => (c1._1+c2._1,c1._2+c1._2)
    )
    //读取的数据为多个分区时,计算结果有问题。。。
    //combineRDD.map { case (name, (num, socre)) => (name, socre / num) }.collect.foreach(println)

    combineRDD.foreach(x => {
      println("类型："+x._1+",对应的数据："+x._2)
    })*/


    //rdd1.reduceByKey(_+_).foreach(println)
    //groupByKey效率要慢于reduceByKey
    //rdd1.groupByKey(1).map(x => (x._1,x._2.sum)).foreach(println)





  }

}