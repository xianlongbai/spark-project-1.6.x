package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by root on 2018/3/19.
  */

//case class Student(
//                    id:Int,
//                    studentName:String,
//                    phone:String,
//                    email:String)
object df_csv2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dfJson").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //import com.databricks.spark.csv._

    //方式二:有点问题
    val options = Map("header" -> "true", "path" -> "StudentData.csv")
    val newStudents: DataFrame = sqlContext.read.options(options).format("com.databricks.spark.csv").load()
    newStudents.printSchema()
    //newStudents.show()'
    newStudents.registerTempTable("student")
    sqlContext.sql("select * from student").show()
  }
}
