package com.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2018/3/19.
  *
  *  ssc.stop()   现有程序将会优雅的关闭
  *
  * nc -lk 9999
  */
object wcOnline {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wcOnline")
    //开启预写日志，开启 WAL容错机制
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    //创建一个StreamingContext对象，并指定Batch Interval的间隔是5s，也就是说每经过5s，
    //SparkStreaming会将这5s内的信息封装成一个DStream,然后提交到Spark集群进行计算
    val ssc = new StreamingContext(sparkConf,Durations.seconds(5))
    //设置持久化的路径，也可以设置为hdfs的路径，如果设置为hdfs，就不需要设置MEMORY_AND_DISK_2
    ssc.checkpoint("D:\\tmp\\sparkstreaming")
    val linesDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node2", 9999, StorageLevel.MEMORY_AND_DISK);
    val wordsDStream: DStream[String] = linesDStream.flatMap { _.split(" ") }
    val pairDStream: DStream[(String, Int)] = wordsDStream.map { (_,1) }
    val resultDStream = pairDStream.reduceByKey(_+_)
    resultDStream.print()
    //启动app应用程序
    //JavaStreamingContext.stop() 停止之后是不能在调用start
    ssc.start()
    //表示循环执行这段代码，等待被中断，相当于while(true)
    ssc.awaitTermination()
    //JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭
    //而stop(false)则只会关闭StreamingContext，不会关闭内部封装的SparkContext（如果接下来的程序要用到这个对象）
    //ssc.stop()
  }
}
