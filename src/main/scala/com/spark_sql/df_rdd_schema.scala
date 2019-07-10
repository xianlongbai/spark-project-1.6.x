package com.spark_sql

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.IntegerType

/**
  * Created by root on 2018/3/18.
  * 这种方式，不需要事先知道schema
  *
  *（schema和数据 分别存储，使用的时候直接创建dataframe）
  */
object df_rdd_schema {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("df_rdd_model")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //这里的schema可以存放在表中或者hdfs上
    val schemaStr = "name sex age brithday"
    val lineRdd = sc.textFile("student.txt")

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType,StructField,StringType}

    //创建schema
    val schema: StructType = StructType(schemaStr.split(" ").map(fieldName =>
        StructField(fieldName, if (!"age".equals(fieldName)) StringType else IntegerType, true)))
    //将rdd记录转为rowRDd
    val rowRdd: RDD[Row] = lineRdd.map((str: String) => str.split(",")).map(x => Row(x(0),x(1),x(2).trim.toInt,x(3)))
    //引用schema
    val rel: DataFrame = sqlContext.createDataFrame(rowRdd,schema)
    rel.printSchema()
    rel.registerTempTable("student")
    sqlContext.sql("select * from student").show()


  }
}
