package com.spark.spark_sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2019/7/15.
  * 读取json为DataFrame
  */
class Createdf {

}
object Createdf{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val jsonRDD: DataFrame = ssc.read.json("people.txt")
    jsonRDD.registerTempTable("people")
    jsonRDD.printSchema()
    //val scheamStr: StructType = jsonRDD.schema
    val head: Array[Row] = jsonRDD.head(2)  //读取前几行
    val etlrdd: DataFrame = ssc.sql("select * from people where age > 20").cache()
    jsonRDD.show(2,false)
    val sss: DataFrame = etlrdd.limit(100)
    etlrdd.write.partitionBy("age").mode(SaveMode.Overwrite).format("parquet").save("D:\\tmp\\spark_sql\\parquet")
    //仅支持hiveContext
    //etlrdd.write.mode(SaveMode.Overwrite).format("orc").save("D:\\tmp\\spark_sql\\orc")
    //仅支持单列df
    //etlrdd.write.mode(SaveMode.Overwrite).format("text").save("D:\\tmp\\spark_sql\\text")
    etlrdd.write.mode(SaveMode.Overwrite).format("json").save("D:\\tmp\\spark_sql\\json")

    etlrdd.rdd.saveAsTextFile("D:\\tmp\\spark_sql\\text")








  }
}