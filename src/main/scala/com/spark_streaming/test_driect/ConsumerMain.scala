package com.spark_streaming.test_driect

import kafka.serializer.StringDecoder
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Created by root on 2018/4/6.
  */
object ConsumerMain extends Serializable {

  //@transient 表示此类不需要序列化
  @transient lazy val log = LogManager.getRootLogger

  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster("local[2]")
//      .set("spark.local.dir", "~/tmp")
//      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test01".split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String](
                      "metadata.broker.list" -> "node1:9092,node2:9092,node3:9092",
                      "auto.offset.reset" -> "smallest",
                      "group.id" -> "test_direct")
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream: InputDStream[(String, String)] = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")
    kafkaDirectStream.cache
    //do something......
    kafkaDirectStream.print()
    //更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })
    ssc
  }

  def main(args: Array[String]) {
    val ssc = functionToCreateContext()
    ssc.start()
    ssc.awaitTermination()
  }
}