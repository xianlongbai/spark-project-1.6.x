package com.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by root on 2018/3/15.
  * 注意：如果数据源为orc格式的数据，只能通过hiveContext 来加载读取
  * 因为本地无法创建hivecontext对象，所以必须打包到客户端执行
  *
  *
  * 性能优化：
  * hsc.cacheTable("merge01")  //缓存表数据据
  * dataframe.cache()
  * sqlContext.uncacheTable("tableName")
  */
object df_orc {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("df_orc")
    //The ORC data source can only be used with HiveContext.
    val sc = new SparkContext(conf)
    val hsc = new HiveContext(sc)
    //直接读取hive表创建df
    //val df0: DataFrame = hsc.table("merge01")
    //val df = hsc.read.orc("hdfs://node1:8020/user/hive_external/merge_orc/000000_0")
    val df = hsc.read.orc("/user/hive_external/merge_orc/000000_0")
    df.registerTempTable("merge")
    df.printSchema()
    hsc.sql("select * from merge limit 10").show()
    hsc.sql("select * from merge limit 10").write.saveAsTable("merge_test")
    hsc.sql("select * from merge limit 10").write.mode(SaveMode.Append).format("json")saveAsTable("merge_test")


  }
}
