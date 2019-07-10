package com.spark_streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
  * Created by root on 2018/3/26.
  *
  * 注意：
  * reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])
  * 在使用此高性能窗口操作时，必须启用checkpoint
  */
object dstream_window {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_window")
    val sc = new SparkContext(conf)
    val ss = new StreamingContext(sc,Durations.seconds(5))

    //每一个batch 都会记录
    ss.checkpoint("D:\\tmp\\checkpoint\\file02")
    val receive: ReceiverInputDStream[String] = ss.socketTextStream("node1",9999)
    val preRdd: DStream[(String, Int)] = receive.flatMap(_.split(" ")).map((_,1))

    //每隔10秒计算一次最近20秒的数据
    //val res: DStream[(String, Int)] = preRdd.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(10))

    val res2: DStream[(String, Int)] = preRdd.reduceByKeyAndWindow((a: Int, b: Int) =>a+b, (c:Int, d:Int) => c-d, Seconds(30), Seconds(10))

    res2.print()

    ss.start()
    ss.awaitTermination()
  }

}
