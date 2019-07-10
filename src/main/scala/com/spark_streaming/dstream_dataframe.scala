package com.spark_streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

/**
  * Created by root on 2018/3/28.
  *
  * 在Streaming应用中可以调用DataFrames and SQL来处理流式数据。
  * 开发者可以用通过StreamingContext中的SparkContext对象来创建一个SQLContext，
  * 并且，开发者需要确保一旦驱动器（driver）故障恢复后，该SQLContext对象能重新创建出来。
  * 同样，你还是可以使用懒惰创建的单例模式来实例化SQLContext，如下面的代码所示，
  * 这里我们将最开始的那个小栗子做了一些修改，使用DataFrame和SQL来统计单词计数。
  * 其实就是，将每个RDD都转化成一个DataFrame，然后注册成临时表，再用SQL查询这些临时表。
  *
  */

object getSparkSqlInstant{

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


object dstream_dataframe {

  def main(args: Array[String]): Unit = {

//    if (args.length < 2) {
//      System.err.println("Usage: NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

    val conf = new SparkConf().setAppName("object dstream_accumulators").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))   // new context

    val receive: ReceiverInputDStream[String] = ssc.socketTextStream("node1",9999)
    val word: DStream[String] = receive.flatMap(_.split(" "))

    word.foreachRDD((value: RDD[String]) =>{

      //val sqlContext = SQLContext.getOrCreate(value.sparkContext)
     val sqlContext = getSparkSqlInstant.getInstance(value.sparkContext)

      //注意这里！！！！
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame: DataFrame = value.toDF("word")
      wordsDataFrame.registerTempTable("word")

      sqlContext.sql("select word,count(1) from word group by word").show()

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
