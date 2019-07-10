package com.spark_streaming

import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2018/3/28.
  *
  * 注意：
  * 首先需要注意的是，累加器（Accumulators）和广播变量（Broadcast variables）是无法从Spark Streaming的检查点中恢复回来的。
  * 所以如果你开启了检查点功能，并同时在使用累加器和广播变量，那么你最好是使用懒惰实例化的单例模式，
  * 因为这样累加器和广播变量才能在驱动器（driver）故障恢复后重新实例化。
  */
object dstream_accumulators {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("object dstream_accumulators").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))   // new context

    val receive1: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val ss1: DStream[(String, Int)] = receive1.flatMap(_.split(" ")).map((_,1))

    ss1.foreachRDD((value: RDD[(String, Int)]) =>{
      val brod: Broadcast[String] = WordBlacklist.getInstance(value.sparkContext)
      value.filter((tuple: (String, Int)) =>{
        !tuple._1.contains(brod.value)
      }).foreach((tuple: (String, Int)) =>{
        println(brod)
        println(tuple)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  //实现了懒加载
  object WordBlacklist {

    @volatile private var instance: Broadcast[String] = null

    def getInstance(sc: SparkContext): Broadcast[String] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            val wordBlacklist = "aaa"
            instance = sc.broadcast(wordBlacklist)
            println("---------------------进来了--------------------------")
          }
        }
      }
      instance
    }
  }

  object DroppedWordsCounter {

    @volatile private var instance: Accumulator[Long] = null

    def getInstance(sc: SparkContext): Accumulator[Long] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.accumulator(0L, "WordsInBlacklistCounter")
          }
        }
      }
      instance
    }
  }



}
