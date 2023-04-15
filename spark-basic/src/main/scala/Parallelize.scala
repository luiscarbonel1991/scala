package com.devlach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Parallelize {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Parallelize")
      .getOrCreate()

    val data = Seq(1, 2, 3, 4, 5)
    val distData: RDD[Int] = spark.sparkContext.parallelize(data)
    val rddCollect = distData.collect()
    println("Number of partitions: " +  distData.getNumPartitions)
    println("Action: First element: " + distData.first())
    println("Action: RDD elements: " + rddCollect.mkString(", "))


  }

}
