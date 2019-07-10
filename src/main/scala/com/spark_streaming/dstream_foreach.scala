package com.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext, Time}



/**
  * Created by root on 2018/3/25.
  *
  * 这路径如果hdfs的路径 你直接hadoop fs  -put 到你的监测路径就可以，
  * 如果是本地目录用file:///home/data 你不能移动文件到这个目录，
  * 必须用流的形式写入到这个目录形成文件才能被监测到
  *
  * 注意：如果往文件追加内容时，时无法检测到的
  *
  *
  * foreachRDD 为 transformat 算子
  */
object dstream_foreach {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("dstream_foreach")
    val ss = new StreamingContext(conf,Durations.seconds(5))

    val line: DStream[String] = ss.textFileStream("D:\\tmp\\temp_txt")
    val res: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    res.foreachRDD((attr: RDD[(String, Int)]) =>{
      attr.foreach(println)
    })

    ss.start()
    ss.awaitTermination()

  }


}
