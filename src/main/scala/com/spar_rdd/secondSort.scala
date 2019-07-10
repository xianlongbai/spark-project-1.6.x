package com.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2018/3/12.
  * Ordered特质定义了相同类型间的比较方式，但这种内部比较方式是单一的；
Ordering则是提供比较器模板，可以自定义多种比较方式
  *
  */
object secondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("secondSort")
    val sc = new SparkContext(conf)

    val lineRdd = sc.textFile("secondSort.txt")
    val objRdd: RDD[(SecondSorybyKey, String)] = lineRdd.map(x => (new SecondSorybyKey(x.split(" ")(0).toInt,x.split(" ")(1).toInt),x))
    val relRdd: RDD[(SecondSorybyKey, String)] = objRdd.sortByKey(false)
    relRdd.foreach((tuple: (SecondSorybyKey, String)) =>{
      println(tuple._2)
    })
  }

}

case class SecondSorybyKey(val first:Int,val second:Int)extends Ordered[SecondSorybyKey] with Serializable{

  override def compare(that: SecondSorybyKey): Int = {
    if (this.first-that.first==0){
      this.second-that.second
    }else{
      this.first-that.first
    }
  }
}


//case class SecondSorybyKey2(val first:Int,val second:Int)extends Ordering[SecondSorybyKey2] with Serializable{
//  override def compare(x: SecondSorybyKey2, y: SecondSorybyKey2): Int = {
//    if (x.first == y.first){
//      x.second - y.second
//    }else{
//      x.first - y.first
//    }
//  }
//}