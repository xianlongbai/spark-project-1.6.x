package com.spark_streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
  * Created by root on 2018/4/6.
  *
  * 利用direct模式  消费kafka中数据，offset回写zookeeper中（利用的是simpeleKafkaApi）
  *
  * 好处：
  *     Spark Streaming  +Kafka 使用底层API直接读取Kafka的Partition数据，正常Offset存储在CheckPoint中。
  *     但是这样无法实现Kafka监控工具对Kafka的监控，所以手动更新Offset到Zookeeper集群中
  *
  * TopicAndPartition是对 topic和partition的id的封装的一个样例类
  *
  */
object exactlyOnce_kafka extends Serializable{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("exactlyOnce_kafka")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Durations.seconds(5))

    val zkHost = "node2:2181,node3:2181,node4:2181"
    val zkClient = new ZkClient(zkHost)
    val brokerList = "node1:9092,node2:9092,node3:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
                                          "zookeeper.connect" -> zkHost,
                                          "group.id" -> "exactlyOnce")

    /**
      *   val topic: String,
      *   val partition: Int,
      *   val fromOffset: Long,
      *   val untilOffset: Long
      *
      *   OffsetRange 是对topic name，partition id，fromOffset（当前消费的开始偏移），untilOffset（当前消费的结束偏移）的封装。
      *   所以OffsetRange 包含信息有：topic名字，分区Id，开始偏移，结束偏移
      *
      */
    var offsetRanges = Array[OffsetRange]()
    val topic="test01"
    val topicDirs = new ZKGroupTopicDirs("test01_exactlyOnce", topic)  //创建一个消费者组，并指定要消费的topic
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")     //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    //TopicAndPartition是对 topic和partition的id的封装的一个样例类
    var fromOffsets: Map[TopicAndPartition, Long] = Map()   //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    var kafkaStream: InputDStream[(String, String)] = null
    /**
      * 如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      * 以为如果保存的offset小于最小的offset,则会报错
      *
      */
    if (children > 0) {
      for (i <- 0 until children) {
        //读取消费者组下各个partition的offset
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中,得到了当前消费的开始偏移
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      /**
        * 这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
        *
        * case class MessageAndMetadata[K, V](topic: String, partition: Int,
        *                            private val rawMessage: Message, offset: Long,
        *                            keyDecoder: Decoder[K], valueDecoder: Decoder[V])
        */
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //如果未保存(没有发下offset)，根据 kafkaParam 的配置使用最新或者最旧的 offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test01"))
    }

//    kafkaStream.transform{ rdd=>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
//      rdd
//    }.map(x => (x._1+"@"+x._2)).print()

    kafkaStream.transform{ rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.foreachRDD(rdd=>{
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
      rdd.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
