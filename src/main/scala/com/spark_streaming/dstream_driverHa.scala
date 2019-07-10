package com.spark_streaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Created by root on 2018/3/27.
  *
    *Spark standalone or Mesos with cluster deploy mode only:
  *  在提交application的时候  添加 --supervise 选项  如果Driver挂掉 会自动启动一个Driver
  *  但如果任务时运行在yarn cluster 上,重启driver进程的责任就交由resourcemanager
  *
  *  注意：
  *  1、所有以客户端提交的应用程序，一旦driver挂掉，是无法重启的。
  *  2、即便driver重新启动，也还得保证记录了任务执行的进度，也就是元数据，其中包括：
  *       --Configuration   用于创建SparkStreaming应用程序的配置
  *       --DStream operations    定义Dstream的操作逻辑
  *       --Incomplete batches    那些在排队的job但还没有完成的批次。也就是job的执行进度
  *
  *  什么时候启用checkpoint?
  *  1、使用优化窗口操作时
  *  2、避免driver重启导致的数据重新计算（官方：从已有的元数据重新启动一个driver）
  *
  *  代码层面如如何重启失败的driver(恢复所需要的所有元数据)？
  *   1、当程序第一次启动时，它将创建一个新的StreamingContext，设置所有的流，然后调用start（）
  *   2、当程序在失败后重新启动时，它将从检查点目录中的检查点数据重新创建一个StreamingContext。
  *
  *   注意区分 数据本身和元数据的checkpoint区别：
  *     1、元数据的checkpoint是用来恢复当驱动程序失败的场景下
        2、而数据本身或者RDD的checkpoint通常是用来容错有状态的数据处理失败的场景
  *
  *   checkpoint时间间隔设置
  *     1、通常，一个DStream的5-10个滑动间隔的检查点间隔是一个很好的尝试。
  *
  *   当dstream的执行逻辑改变时，有可能发生错误或者依然使用之前的执行逻辑，怎么办？
  *     1、删除checkpoint数据    （有可能出现重复消费数据的情况）
  *     2、指定新的checkpoint路径
  *
  *
  */
object dstream_driverHa {

  val checkpointDir = "D:\\tmp\\checkpoint\\file03"

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("dstream_driverHa").setMaster("local[1]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))   // new context
    val lines: DStream[String] = ssc.textFileStream("D:\\tmp\\temp_txt") // create DStreams
    val kvStream: DStream[(String, Int)] =  lines.flatMap(_.split(" ")).map((_,1))
    ssc.checkpoint(checkpointDir)   // set checkpoint directory
    kvStream.checkpoint(Durations.seconds(20))//设置时间间隔
    println("进来了-----------------------------------！！！")
    kvStream.print()
    ssc
  }

  def main(args: Array[String]): Unit = {

    //
    val ssc = StreamingContext.getOrCreate(checkpointDir,functionToCreateContext)
    ssc.start()
    ssc.awaitTermination()

  }


}
