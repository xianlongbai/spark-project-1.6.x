package com.spark_streaming

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}


/**
  * sparkstreaming的数据输入来源可以基于File,HDFS,Flume,Kafka-socket等
  * Created by root on 2018/4/2.
  *
  * 这种方法使用接收器来接收数据。接收方是使用卡夫卡高级消费者API实现的。与所有的接收器一样，
  * 从卡夫卡通过接收器接收到的数据存储在Spark执行人中，然后由Spark流启动的作业处理数据。
  * 这种方法使用接收器来接收数据。接收方是使用卡夫卡高级消费者API实现的。与所有的接收器一样，
  * 从卡夫卡通过接收器接收到的数据存储在Spark执行人中，然后由Spark流启动的作业处理数据。
  *
  *
  * 1、Kafka中topic的partition与Spark中RDD的partition是没有关系的，因此，在KafkaUtils.createStream()中，
  * 提高partition的数量，只会增加Receiver的数量，也就是读取Kafka中topic partition的线程数量，不会增加Spark处理数据的并行度。
  * 2、可以创建多个Kafka输入DStream，使用不同的consumer group和topic，来通过多个receiver并行接收数据。
  * 3、如果基于容错的文件系统，比如HDFS，启用了预写日志机制，接收到的数据都会被复制一份到预写日志中。因此，在KafkaUtils.createStream()中，设置的持久化级别是StorageLevel.MEMORY_AND_DISK_SER。
  *
  * 特点：
  *   接收到的数据被存储在Spark workers/executors中的内存，同时也被写入到WAL中。只有接收到的数据被持久化到log中，Kafka Receivers才会去更新Zookeeper中Kafka的偏移量。
      接收到的数据和WAL存储位置信息被可靠地存储，如果期间出现故障，这些信息被用来从错误中恢复，并继续处理数据。
  *
  * 缺点：
  *   1、为了数据安全不丢失，开启wal机制，导致程序运行效率低下，数据被存储在excuter和日志中
  *   2、
  *
  */
object receiver_kafka {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("receiver_kafka")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true") //优雅的关闭程序
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Durations.seconds(5))

    ssc.checkpoint("D:\\tmp\\checkpoint\\file05")
    val topics = Map("test01" -> 2)
    //第一个参数是StreamingContext
    //第二个参数是ZooKeeper集群信息（接受Kafka数据的时候会从Zookeeper中获得Offset等元数据信息）
    //第三个参数是Consumer Group
    //第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
    val receiveDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "192.168.249.102:2181,192.168.249.103:2181,192.168.249.104:2181", "spark_receiver", topics)
    val orgDStream: DStream[(String, String)] = receiveDstream.cache()
    val lines: DStream[String] = orgDStream.map(_._2)
    val key: DStream[String] = orgDStream.map(_._1)
    lines.flatMap(_.split(" ")).map((_,1)).print()
    key.print()  //key是空的
    ssc.start()
    ssc.awaitTermination()
  }

}
