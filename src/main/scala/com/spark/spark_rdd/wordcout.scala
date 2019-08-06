package com.spark.spark_rdd

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec, Lz4Codec, SnappyCodec}
import org.apache.hadoop.mapred.TextOutputFormat

/**
  * Created by root on 2019/7/9.
  */
object wordcout {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")
    val sc = new SparkContext(conf)

    //1、读取单个文件
    //2、读取多个文件,逗号隔开
    //3、读取一个文件夹
    //4、读取嵌套的文件夹下的文件（hdfs://172.20.20.17/tmp/*/*）
    //5、读取本地文件（file:///root/Downloads/data/）
    //6、使用通配符读取文件（hdfs://172.20.20.17/tmp/*.txt）
    //sc.textFile("wc.txt,wc2.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)

    //如果使用rdd1.saveAsTextFile(“file:///tmp/lxw1234.com”)将文件保存到本地文件系统，那么只会保存在Executor所在机器的本地目录
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("D:\\tmp\\spark_rdd\\res_01")
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).foreach(println)
    //val array: Array[(String, Int)] = sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).collect()
    //val disRdd: RDD[String] = sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).map(_._1)
    //基于java序列化保存文件
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).saveAsObjectFile("D:\\tmp\\spark_rdd\\res_02")
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("D:\\tmp\\spark_rdd\\res_03",classOf[Lz4Codec])
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsSequenceFile("D:\\tmp\\spark_rdd\\res_04")

//    sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("D:\\tmp\\spark_rdd\\res_05",classOf[GzipCodec])
    sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("D:\\tmp\\spark_rdd\\res_06",classOf[BZip2Codec])


    // todo ...
    //数据以压缩格式写入hdfs上
    //sc.textFile("wc.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //      .saveAsHadoopFile("/user/testfile/", classOf[Text],classOf[IntWritable],
    //        classOf[TextOutputFormat[Text,IntWritable]], classOf[Lz4Codec])

    //数据写入到hbase
//    var conf = HBaseConfiguration.create()
//    var jobConf = new JobConf(conf)
//    jobConf.set("hbase.zookeeper.quorum","zkNode1,zkNode2,zkNode3")
//    jobConf.set("zookeeper.znode.parent","/hbase")
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"lxw1234")
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    var rdd1 = sc.makeRDD(Array(("A",2),("B",6),("C",7)))
//    rdd1.map(x => {
//      var put = new Put(Bytes.toBytes(x._1))
//      put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
//      (new ImmutableBytesWritable,put)
//    }
//    ).saveAsHadoopDataset(jobConf)

  }

}
