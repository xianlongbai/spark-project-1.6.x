package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by root on 2018/3/19.
  * 从csv文件创建DataFrame主要包括以下几步骤：
  * 　　1、在build.sbt文件里面添加spark-csv支持库；
  * 　　2、创建SparkConf对象，其中包括Spark运行所有的环境信息；
  * 　　3、创建SparkContext对象，它是进入Spark的核心切入点，然后我们可以通过它创建SQLContext对象；
  * 　　4、使用SQLContext对象加载CSV文件；
  * 　　5、Spark内置是不支持解析CSV文件的，但是Databricks公司开发了一个类库可以支持解析CSV文件。所以我们需要把这个依赖文件加载到依赖文件中（pom.xml或者是build.sbt）
  *
  */
object df_csv {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dfJson").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import com.databricks.spark.csv._

    //方式一：
    val students: DataFrame = sqlContext.csvFile(filePath="StudentData.csv", useHeader=true, delimiter='|')
    students.printSchema()
    students.head(3).foreach(println)
    students.registerTempTable("student")
    sqlContext.sql("select * from student limit 10").show()



  }
}
//spark-assembly-1.6.0-hadoop2.6.0.jar