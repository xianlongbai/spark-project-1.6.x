package com.spark_streaming

import java.io.{File, PrintWriter}

import org.apache.spark.sql.catalyst.expressions.InputFileName

import scala.io.{BufferedSource, Source}



/**
  * Created by root on 2018/3/25.
  */
class writeFile {

}

object writeFile{

  def main(args: Array[String]): Unit = {

    val writer = new PrintWriter(new File("D:\\tmp\\temp_txt\\wc.txt" ))
    writer.write("yanzi hello shiwang love")
    writer.write("hello bxl bxl love")
    writer.write("hello hello love yanzi")
    writer.close()

  }
}
