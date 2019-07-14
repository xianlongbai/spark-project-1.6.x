package com.spark.spark_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2019/7/14.
  */
object mapTest {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val array = Array("aaa", "bbb", "ccc", "ddd", "eee", "ffff")

    val dataArr = Array("Tom01","Tom02","Tom03"
                        ,"Tom04","Tom05","Tom06"
                        ,"Tom07","Tom08","Tom09"
                        ,"Tom10","Tom11","Tom12")

    val list: Seq[(String, Int)] = List(
                    ("bjsxt",1),
                    ("bjsxt",2),
                    ("shsxt",1)
                  )

    val count = sc.accumulator(0)
    val count2 = sc.accumulator(0)

    //sc.parallelize(array,2).map(_+"|").foreach(println)

    /*val parttionRdd: RDD[String] = sc.makeRDD(array, 3).mapPartitions(x => {
      //创建可变的list，方法是使用ListBuffer，再将ListBuffer转化为List。分区数据量大会导致OOM
      val array = new ListBuffer[String]()
      while (x.hasNext) {
        count.add(1)
        array.append(x.next())
      }
      count2.add(1)
      array.iterator
    }, false)

    parttionRdd.foreach(println)
    println(count)
    println(count2)*/

    //    val input = sc.wholeTextFiles("D:\\tmp\\data_set")
    //    input.mapValues(x => {
    //      var res = ""
    //      for (elem <- x.split("\r\n").map(_ + "@")) {
    //        res = res + elem
    //      }
    //      res
    //    }).foreach(println)

//    val links = sc.parallelize(List(("A","Q"),("B","w"),("C","r"),("D","T"))).partitionBy(new HashPartitioner(2)).persist()
//    var ranks=links.mapValues(v=>1.0)
//    val results = ranks.collect()
//    for(result<- results){
//      println(result)
//    }

//    val rdd = sc.parallelize(dataArr, 3);
//    val result = rdd.mapPartitionsWithIndex((index,x) => {
//      val list = ListBuffer[String]()
//      while (x.hasNext) {
//        list += "partition:"+ index + " content:" + x.next
//      }
//      list.iterator
//    })
//    println("分区数量:" + result.partitions.size)
//    //val resultArr = result.collect()
//    for(x <- result){
//      println(x)
//    }





  }


}
