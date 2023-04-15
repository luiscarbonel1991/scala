package com.devlach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.Console.println

object ReadMultipleTextFiles {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSession("Read Multiple Text Files")
    val pathFiles = "absolute_path..../txt-files/*"
    val rdd = spark.sparkContext.textFile(pathFiles)
    println("Using textFile: ")
    rdd.foreach(println) // Print the content of the RDD
    val rddWhole = spark.sparkContext.wholeTextFiles(pathFiles)

    println("Using wholeTextFiles: ")
    rddWhole.foreach( f => println(f._1 + " " + f._2)) // Print the content of the RDD
  }

}
