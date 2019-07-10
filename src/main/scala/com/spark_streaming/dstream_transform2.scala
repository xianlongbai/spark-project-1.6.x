package com.spark_streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2018/3/25.
  */
object dstream_transform2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_transform2")
    val sc = new SparkContext(conf)
    val ss = new StreamingContext(sc,Durations.seconds(5))

    val array1 = Array(("bxl",1),("zsy",2),("zxx",3))
    val rdd1: RDD[(String, Int)] = sc.parallelize(array1)

    val receive: ReceiverInputDStream[String] = ss.socketTextStream("node1",9999)
    val res: DStream[(String, (Int, Int))] = receive.transform((value: RDD[String]) =>{
      value.flatMap(_.split(" ")).map((_,1)).join(rdd1)
    })
    res.print()
    ss.start()
    ss.awaitTermination()
  }
}
