package com.spark_streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2018/3/25.
  *  * 1、local的模拟线程数必须大于等于2 因为一条线程被receiver(接受数据的线程)占用，另外一个线程是job执行
  * 2、Durations时间的设置，就是我们能接受的延迟度，这个我们需要根据集群的资源情况以及监控，ganglia  每一个job的执行时间
  * 3、 创建JavaStreamingContext有两种方式 （sparkconf、sparkcontext）
  * 4、业务逻辑完成后，需要有一个output operator
  * 5、JavaStreamingContext.start()
  * 6、JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false)
  * 7、JavaStreamingContext.stop() 停止之后是不能在调用start
  * 8、JavaStreamingContext.start() straming框架启动之后是不能在次添加业务逻辑
  */
object dstream_transform {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_transform")
    val sc = new SparkContext(conf)
    val ss = new StreamingContext(sc,Durations.seconds(5))

    val black: Broadcast[String] = sc.broadcast("tiger")

    val receive: ReceiverInputDStream[String] = ss.socketTextStream("node1",9999)
    val resRdd: DStream[String] = receive.transform((attr: RDD[String]) =>{
      attr.filter(!_.contains(black.value))
    })
    resRdd.print()
    ss.start()
    ss.awaitTermination()

  }

}
