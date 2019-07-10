package com.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * Created by root on 2018/3/28.
  * 需求：同时统计两个流的最近30的单词个数
  *
  */
object dstream_union {

  def main(args: Array[String]): Unit = {

    //注意，这里有2个接口数据的线程，加上打印线程，需要启动三个线程
    val conf = new SparkConf().setMaster("local[3]").setAppName("dstream_union")
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    ssc.checkpoint("D:\\tmp\\checkpoint\\file04")

    val receive1: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val receive2: ReceiverInputDStream[String] = ssc.socketTextStream("node2",9999)

    val ss1: DStream[(String, Int)] = receive1.flatMap(_.split(" ")).map((_,1))
    val ss2: DStream[(String, Int)] = receive2.flatMap(_.split(" ")).map((_,1))

    val sum1: DStream[(String, Int)] = ss1.reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2, (v1:Int, v2:Int)=>v1-v2,Durations.seconds(30),Durations.seconds(10))
    val sum2: DStream[(String, Int)] = ss2.reduceByKeyAndWindow((v1:Int, v2:Int)=>v1+v2, (v1:Int, v2:Int)=>v1-v2,Durations.seconds(30),Durations.seconds(10))

    val sumRel: DStream[(String, Int)] = sum1.union(sum2).reduceByKey(_+_)

    sumRel.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
