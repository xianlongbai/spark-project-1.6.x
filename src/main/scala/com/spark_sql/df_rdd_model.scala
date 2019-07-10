package com.spark_sql

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/18.
  *
  * 这种方式适合事先知道schema的数据
  *
  * 注意：在scala中利用反射将一个rdd转为df,一定要创建导入 import sqlContext.implicits._
  *   --这被用来隐式地将RDD转换为DataFrame。
  *
  *    原因：在Spark 1.3之前的许多代码示例都是从导入sqlContext开始的。它将sqlContext中的所有函数都带入了范围。
  *    在Spark 1.3中，我们分离了将RDDs转换成DataFrames的隐式转换，并将其转换成SQLContext中的对象。
  *    用户现在应该写import sqlContext.implicits
  *       另外，隐式转换现在只增加由产品组成的RDDs（也就是用一个方法toDF，而不是自动应用的方法，case类或元组）。
  *
  *
  *
  */

case class Student(
                    name:String,
                    sex:String,
                    age:Int,
                    brithday:String)

object df_rdd_model {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("df_rdd_model")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //注意这里。。。
    import sqlContext.implicits._

    val lineRdd = sc.textFile("student.txt")
    //val objRDD: RDD[Student] = lineRdd.map(_.split(",")).map((strs: Array[String]) => Student(strs(0),strs(1),strs(2).toInt,strs(3)))
    val objRDD: RDD[Student] = lineRdd.map((str: String) =>Student(str.split(",")(0),str.split(",")(1),str.split(",")(2).toInt,str.split(",")(3)))

    val objdf: DataFrame = objRDD.toDF()
    objdf.printSchema()
    objdf.registerTempTable("student")
    val subdf: DataFrame = sqlContext.sql("select * from student where sex = '男'")
    subdf.show()
    //对dataframe调用map算子后，返回类型为RDD<Row>
    //在结果中，可以通过列名访问结果中的列
    subdf.map((r : Row) => r.getAs[String]("name")+"好帅").foreach(println)
    //SQL查询的结果是DataFrames并支持所有正常的RDD操作
    //在结果中，可以通过字段索引访问结果中的列
    subdf.map((row: Row) =>row.get(1)+"人").foreach(println)
    //行。getValuesMap T一次性检索多个列到地图字符串T中
    subdf.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    sc.stop()

  }
}
