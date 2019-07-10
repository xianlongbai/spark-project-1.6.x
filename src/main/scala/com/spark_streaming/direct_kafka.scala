package com.spark_streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/4/2.
  *
  * 不同于Receiver的方式，Direct方式没有receiver这一层，其会周期性的获取Kafka中每个topic的每个partition中的最新offsets，之后根据设定的maxRatePerPartition来处理每个batch
  *
  * 优点：
  *    1、简化的并行：在Receiver的方式中我们提到创建多个Receiver之后利用union来合并成一个Dstream的方式提高数据传输并行度。而在Direct方式中，Kafka中的partition与RDD中的partition是一一对应的并行读取Kafka数据，这种映射关系也更利于理解和优化。
       2、高效：在Receiver的方式中，为了达到0数据丢失需要将数据存入Write Ahead Log中，这样在Kafka和日志中就保存了两份数据，浪费！而第二种方式不存在这个问题，只要我们Kafka的数据保留时间足够长，我们都能够从Kafka进行数据恢复。
       3、精确一次：在Receiver的方式中，使用的是Kafka的高阶API接口从Zookeeper中获取offset值，这也是传统的从Kafka中读取数据的方式，但由于Spark Streaming消费的数据和Zookeeper中记录的offset不同步，这种方式偶尔会造成数据重复消费。而第二种方式，直接使用了简单的低阶Kafka API，Offsets则利用Spark Streaming的checkpoints进行记录，消除了这种不一致性。

  *
  */
object direct_kafka {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("spark_streaming")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("D:\\tmp\\checkpoint\\file06")
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(5))

    // val topics = Map("spark" -> 2)

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092",
      "group.id" -> "spark_direct",
      "auto.offset.reset" -> "smallest"
    )
    // 直连方式拉取数据，这种方式不会修改数据的偏移量，需要手动的更新
    val lines =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test01")).map(_._2)
    //val lines = KafkaUtils.createStream(ssc, "m1:2181,m2:2181,m3:2181", "spark", topics).map(_._2)

    val ds1 = lines.flatMap(_.split(" ")).map((_, 1))

    val ds2 = ds1.updateStateByKey[Int]((x:Seq[Int], y:Option[Int]) => {
      Some(x.sum + y.getOrElse(0))
    })

    ds2.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
