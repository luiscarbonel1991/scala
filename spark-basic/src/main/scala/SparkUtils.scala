package com.devlach.spark

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSession(appName: String) = {
    SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
  }

}
