package com.spark.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{udf, lit}

import scala.util.Try

/**
  * Created by root on 2019/7/19.
  */


case class SubRecord(x: Int)
case class ArrayElement(foo: String, bar: Int, vals: Array[Double])
case class Record(
                   an_array: Array[Int],
                   a_map: Map[String, String],
                   a_struct: SubRecord,
                   an_array_of_structs: Array[ArrayElement]
                 )

//Scala的Seq将是Java的List，Scala的List将是Java的LinkedList。
object df_difficult {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._
    val df: DataFrame = sc.parallelize(Seq(
      Record(
        Array(1, 2, 3),
        Map("foo" -> "bar"),
        SubRecord(1),
        Array(ArrayElement("foo", 1, Array(1.0, 2.0)), ArrayElement("bar", 2, Array(3.0, 4.0)))
      ),
      Record(
        Array(4, 5, 6),
        Map("foz" -> "baz"),
        SubRecord(2),
        Array(ArrayElement("foz", 3, Array(5.0, 6.0)), ArrayElement("baz", 4, Array(7.0, 8.0))))
    )).toDF

    df.registerTempTable("df_table")
    df.printSchema()

    //df.select($"an_array".getItem(2)).show()
    println("------------------------------------")
    //sqlContext.sql("SELECT an_array[1] FROM df_table").show
    //sqlContext.sql("SELECT a_map['foo'] FROM df_table").show()


    //udf函数的使用
    val get_ith = udf((xs: Seq[Int], i: Int) => Try(xs(i)).toOption)
    //df.select(get_ith($"an_array", lit(0))).show

//    df.select($"a_struct.x").show
//    sqlContext.sql("SELECT a_struct.x FROM df_table").show

//    df.select($"an_array_of_structs.foo").show
    sqlContext.sql("SELECT an_array_of_structs[0].foo FROM df_table").show(false)
    //df.select($"an_array_of_structs.vals".getItem(1).getItem(1)).show

  }







}
