package com.spark.spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/14.
  */
object joinTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val nameList = Array(Tuple2(1,"xuruyun"),Tuple2(2,"liangyongqi"),Tuple2(3,"wangfei"))
    val scoreList = Array(Tuple2(1,150),Tuple2(2,100),Tuple2(2,90))

    val name = sc.parallelize(nameList,3)
    val score = sc.parallelize(scoreList,2)

    val rdd1 = sc.parallelize(Array(1,2,3))
    val rdd2 = sc.parallelize(Array(3,4,5,6))

    val jrdd1: RDD[(Int, (String, Int))] = name.join(score)
    val jrdd2: RDD[(Int, (String, Option[Int]))] = name.leftOuterJoin(score)
    val jrdd3: RDD[(Int, (Option[String], Int))] = name.rightOuterJoin(score)
    val jrdd4: RDD[(Int, (Option[String], Option[Int]))] = name.fullOuterJoin(score)
    println(jrdd1.partitions.size)
    println(jrdd2.partitions.size)
    println(jrdd3.partitions.size)
    println(jrdd4.partitions.size)
    //jrdd1.foreach(println)
//    jrdd2.foreach(x =>{
//      println((x._1,(x._2._1,x._2._2.getOrElse("0"))))
//    })
//    jrdd3.foreach(println)
//    jrdd4.foreach(println)

//    val aRdd: RDD[Int] = rdd1.union(rdd2)
//    aRdd.foreach(println)

    val res: RDD[(Int, (Iterable[String], Iterable[Int]))] = name.cogroup(score)
    res.foreach(println)
    println(res.partitions.size)
    val res2: RDD[(Int, (Iterable[String], Iterable[Int]))] = res.repartition(2)
    println(res2.partitions.size)




  }

}


