package com.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2018/3/28.ji
  *
  * join操作
  *   1、基于kv格式的Dstream
  *   2、
  *
  */
object dstream_join {

  def main(args: Array[String]): Unit = {
    //注意，这里有2个接口数据的线程，加上打印线程，需要启动三个线程
    val conf = new SparkConf().setMaster("local[3]").setAppName("dstream_foreach")
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    val receive1: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val receive2: ReceiverInputDStream[String] = ssc.socketTextStream("node2",9999)

    val ss1: DStream[(String, Int)] = receive1.flatMap(_.split(" ")).map((_,1))
    val ss2: DStream[(String, Int)] = receive2.flatMap(_.split(" ")).map((_,1))

    val win1: DStream[(String, Int)] = ss1.window(Durations.seconds(20),Durations.seconds(5))
    val win2: DStream[(String, Int)] = ss2.window(Durations.seconds(20),Durations.seconds(5))

    val rel: DStream[(String, (Int, Int))] = win1.join(win2)

    rel.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
