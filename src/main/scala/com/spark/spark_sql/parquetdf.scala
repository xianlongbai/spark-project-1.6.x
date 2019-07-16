package com.spark.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by root on 2019/7/15.
  */
class parquetdf {

}

object parquetdf{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

//    val dfRdd: DataFrame = sqlContext.read.parquet("D:\\tmp\\spark_sql\\parquet")
//    dfRdd.printSchema()
//    dfRdd.show(false)

    import sqlContext.implicits._
    val studentsWithNameAge = Array(("john",18),("april",19),("annie",17))
    val studentsWithNameAgeDF: DataFrame = sc.parallelize(studentsWithNameAge).toDF("name","age")
    studentsWithNameAgeDF.save("D:\\tmp\\spark_sql\\merge_parquet", "parquet", SaveMode.Append)

    val studentsWithNameScore = Array(("john",88),("wangfei",99),("liangyongqi",77))
    val studentsWithNameScoreDF = sc.parallelize(studentsWithNameScore).toDF("name","score")
    studentsWithNameScoreDF.save("D:\\tmp\\spark_sql\\merge_parquet", "parquet", SaveMode.Append)

    //合并元数据scheam信息
    val students = sqlContext.read.option("mergeSchema", "true").parquet("D:\\tmp\\spark_sql\\merge_parquet")
    students.printSchema()
    students.show()





  }

}
