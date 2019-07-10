package com.spark_streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

/**
  * Created by root on 2018/3/25.
  * * UpdateStateByKey的主要功能:
  * 1、Spark Streaming中为每一个Key维护一份state状态，state类型可以是任意类型的的，
  * 可以是一个自定义的对象，那么更新函数也可以是自定义的。
  * 2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，
  * Spark Streaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新
  *注意：
  * 如果要不断的更新每个key的state，就一定涉及到了状态的保存和容错，这个时候就需要开启checkpoint机制和功能
  *
  */
object dstream_updateStateByKey {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_transform")
    val sc = new SparkContext(conf)
    val ss = new StreamingContext(sc,Durations.seconds(5))

    /**
      * 多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
      * 	如果你的batch interval小于10s  那么10s会将内存中的数据写入到磁盘一份
      * 	如果bacth interval 大于10s，那么就以bacth interval为准
      */
    ss.checkpoint("D:\\tmp\\checkpoint\\file01");
    val receive: ReceiverInputDStream[String] = ss.socketTextStream("node1",9999)
    val preRdd: DStream[(String, Int)] = receive.flatMap(_.split(" ")).map((_,1))

    //第一个参数表示每个key对应的一堆1  如 hello:[1,1,1,1]
    //第二个参数表示每个key对应的历史记录值
    preRdd.updateStateByKey((values: Seq[Int], status: Option[Int]) =>{
      val sum1 = values.sum
      val sum2 = status.getOrElse(0)
      Some(sum1+sum2)
    }).foreachRDD((value: RDD[(String, Int)]) =>{
      value.foreach(println)
    })
    ss.start()
    ss.awaitTermination()


  }

}
