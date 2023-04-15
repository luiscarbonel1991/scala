package com.devlach.spark

import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Basic")
      .getOrCreate() // Create a SparkSession

    println(session) // Print the name of the application
    println("Spark version:" + session.version) // Print the version of Spark

    session.createDataFrame(Seq(
      (1, "Hello"),
      (2, "World")
    )).toDF("id", "text").show() // Create a DataFrame and print it
  }
}