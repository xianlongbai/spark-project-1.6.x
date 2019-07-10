package com.spark_sql

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by root on 2018/3/18.
  * 模式合并
  * 由于模式合并是一项比较昂贵的操作，而且在大多数情况下不是必需的，所以我们默认从1.5.0开始关闭它。你可以通过
  * --在读取Parquet文件时设置数据源选项合并到true（如下面的例子所示），或者
  * --设置全局SQL选项spark.l.parquet.mergeSchema为true。
  *
  *
  *   message spark_schema {
        optional int32 single;
        optional int32 double;
        optional int32 triple;
      }
  *
  * +------+------+------+---+
    |single|double|triple|key|
    +------+------+------+---+
    |     1|     2|  null|  1|
    |     2|     4|  null|  1|
    |     3|     6|  null|  1|
    |     4|     8|  null|  1|
    |     5|    10|  null|  1|
    |     6|  null|    18|  2|
    |     7|  null|    21|  2|
    |     8|  null|    24|  2|
    |     9|  null|    27|  2|
    |    10|  null|    30|  2|
    +------+------+------+---+
  *
  *
  *
  *
  */
object schemaMerge {

  //hdfs://node1:8020/user/hive_external/merge_parquet/000000_0
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("df_orc").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //创建一个简单的DataFrame，存储到一个分区目录中
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.mode(SaveMode.Ignore).parquet("hdfs://node1:8020/user/bxl/test_table/key=1")
    //在新的分区目录中创建另一个DataFrame，
    //添加一个新列并删除现有的列
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.mode(SaveMode.Ignore).parquet("hdfs://node1:8020/user/bxl/test_table/key=2")
    //读取分区表
    val df3: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://node1:8020/user/bxl/test_table")
    df3.printSchema()
    df3.show()

  }
}
