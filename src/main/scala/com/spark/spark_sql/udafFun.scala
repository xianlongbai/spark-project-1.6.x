package com.spark.spark_sql



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, types}

/**
  * Created by root on 2019/7/16.
  */
class udafFun {

}


object udafFun{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val srdd: RDD[String] = sc.textFile("student.txt")
    val rowRdd: RDD[Row] = srdd.map(_.split(",")).map(x => Row(x(0),x(1),x(2).toInt,x(3).toLong))
    val scheam: StructType = StructType(Array(StructField("name",StringType,false),
                      StructField("sex",StringType,false),
                      StructField("age",IntegerType,false),
                      StructField("brithday",LongType,false)))

    val resRdd: DataFrame = sqlContext.createDataFrame(rowRdd,scheam)
    resRdd.show()

    //定义udf函数
    sqlContext.udf.register("strLen",(x:String) => x.length())

    resRdd.registerTempTable("student")
    sqlContext.sql("select name,sex,age,strLen(name) as size from student").show(false)

    //定义udaf函数：聚合操作
    sqlContext.udf.register("strCount,)



  }

}