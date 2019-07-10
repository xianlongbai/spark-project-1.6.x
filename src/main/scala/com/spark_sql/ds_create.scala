package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * Created by root on 2018/3/19.
  *
  * 大多数常见类型的编码器都是通过导入sqlContext.implicits来自动提供的 (import sqlContext.implicits._)
  *
  */

case class Person(name: String, age: Long)
object ds_create {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("ds_create")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //方式一
    val ds: Dataset[Int] = Seq(1, 2, 3).toDS()
    ds.map(_ + 1).collect().foreach(println)
    //import sqlContext.implicits._
    println("-----------------------------------------------------------------")
    //方式二
    val ds2: Dataset[Person] = Seq(Person("Andy", 32)).toDS()
    ds2.printSchema()
    ds2.toDF().registerTempTable("perple")
    sqlContext.sql("select * from perple").show()
    println("-----------------------------------------------------------------")
    //方式三
    val path = "people.txt"
    val people: Dataset[Person] = sqlContext.read.json(path).as[Person]
    people.show()
  }

}
