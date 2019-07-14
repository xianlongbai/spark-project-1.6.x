package com.spark.spark_rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/14.
  */
class combineTest {

}
object combineTest{

  def main(args: Array[String]): Unit = {

//    举例理解：
//    假设我们要将一堆的各类水果给榨果汁，并且要求果汁只能是纯的，不能有其他品种的水果。那么我们需要一下几步：
//    1 定义我们需要什么样的果汁。
//    2 定义一个榨果汁机，即给定水果，就能给出我们定义的果汁。--相当于hadoop中的local combiner
//    3 定义一个果汁混合器，即能将相同类型的水果果汁给混合起来。--相当于全局进行combiner

//    那么对比上述三步，combineByKey的三个函数也就是这三个功能
//    1 createCombiner就是定义了v如何转换为c
//    2 mergeValue 就是定义了如何给定一个V将其与原来的C合并成新的C
//    3 就是定义了如何将相同key下的C给合并成一个C

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(Array(("A",1),("A",2),("A",3),("A",4),("B",1),("B",2),("C",1)),2)

    //将1转换成 list(1)
    //将list(1)和2进行组合从而转换成list(1,2)
    //将全局相同的key的value进行组合
    rdd1.combineByKey(
      (v : Int) => List(v),
      (c : List[Int], v : Int) => v :: c,
      (c1 : List[Int], c2 : List[Int]) => c1 ::: c2,
      2
    ).foreach(println)




  }

}
