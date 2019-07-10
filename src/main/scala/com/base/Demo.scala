package com.base

import com.base.Demo.a

import scala.collection.immutable.{Queue, Stack}
import scala.collection.mutable.ListBuffer

/**
  * immutable   不可变的    /    mutable  可变的
  *
  * Created by root on 2018/3/13.
  * Scala 集合分为可变的和不可变的集合
  *
  * 可变集合可以在适当的地方被更新或扩展。这意味着你可以修改，添加，移除一个集合的元素。而不可变集合类永远不会改变。
  * 不过，你仍然可以模拟添加，移除或更新操作。但是这些操作将在每一种情况下都返回一个新的集合，同时使原来的集合不发生改变。
  *
  * 特点：
  * 伴生类Person的构造函数定义为private，虽然这不是必须的，却可以有效防止外部实例化Person类，使得Person类只能供对应伴生对象使用；
  * 每个类都可以有伴生对象，伴生类与伴生对象写在同一个文件中；
  * 在伴生类中，可以访问伴生对象的private字段Person.uniqueSkill；
  * 而在伴生对象中，也可以访问伴生类的private方法 Person.getUniqueSkill（）；
  * 在外部不用实例化，直接通过伴生对象访问Person.printUniqueSkill（）方法
  *
  * 使用场景：
  * 作为存放工具函数或常量的地方
  * 高效地共享单个不可变实例
  * 需要使用单个实例来协调某个服务时
  *   独立对象：
  *       作为相关功能方法的工具类，或者定义Scala应用的入口点
  *   伴生对象：
  *       我们通常将伴生对象作为工厂使用
  */
class Demo(tag:String) {
  //不可变集合
  // 定义整型 List
  private val l: Seq[Int] = List(1, 2, 3, 4)
  // 定义 Set
  private val s: Set[Int] = Set(1, 3, 5, 7)
  // 定义 Map
  private val m: Map[String, Int] = Map("one" -> 1, "two" -> 2, "three" -> 3)
  // 创建两个不同类型元素的元组
  private val t: (Int, String) = (10, "Runoob")
  // 定义 Option
  private val o: Option[Int] = None   //Some(5)
  // queue
  private val q = Queue[Int]()
  // stack
  private val sk = Stack[Int]()

  //可变集合
  private var lb: ListBuffer[Int] = new ListBuffer[Int]
  private var que = scala.collection.mutable.Queue[String]()
  private var stack = scala.collection.mutable.Stack[Int]()
  private var ss = scala.collection.mutable.Set[Int]()

  def changeStr(str:String): String = {
    str+"!!!"
  }

}

object Demo{

  def apply(foo: String) = new Demo(foo)
  def changeStr2(str:String): String = {
    str+"!!!"
  }
  private val a = "aaaa"

  def main(args: Array[String]): Unit = {
    //伴生类测试
      val demo = apply("test")
      println(a)
      println(demo.l)
      println(demo.m.getOrElse("aaa",0))
      println(demo.o.getOrElse(0))
      println(demo.changeStr(a))
      Demo.changeStr2("a")
      println("----------------------------")
    //集合测试
      println(demo.l.head)   //返回列表第一个元素
      println(demo.l.tail)  //包含除了第一元素之外的其他元素
      println(demo.l.isEmpty)
      val squares = List.tabulate(6)(n => n * n)
      println( "一维 : " + squares  )
      println(demo.l.reverse)
      println("----------------------------")
      println(demo.lb += 8 += 6)
      println(6 +: demo.lb)
      val queue = demo.q.enqueue(demo.l)
      println("tag1:"+queue)
      val (elem1, que3) = queue.dequeue
      println(elem1)   //移除的元素
      println(que3)    //剩下的队列
      demo.que+="A"
      demo.que++=List("b","c")
      println(demo.que)
      demo.que.dequeue()
      println(demo.que)
      println("------------------------------")
      demo.stack.push(1)
      demo.stack.push(2)
      println(demo.stack)
      println(demo.stack.top)




  }
}

//在外部不用实例化，直接通过伴生对象访问Person.printUniqueSkill（）方法
object duli{
  def main(args: Array[String]): Unit = {
      println(Demo.changeStr2("a"))
  }
}