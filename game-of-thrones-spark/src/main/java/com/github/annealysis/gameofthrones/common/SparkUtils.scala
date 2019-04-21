package com.github.annealysis.gameofthrones.common

import org.apache.spark.sql.SparkSession

/*

Note: this is not my code but borrowed so the program can run.

 */

object SparkUtils {

  def createSparkSession(name: String, highMemoryInstance: Boolean = true): SparkSession = {

    val memoryPerCore = (if(highMemoryInstance) 6500 else 3750) * 0.7
    val driverCores = 4
    val executorCores = 4

    val sparkConfig: Map[String, String] = Map(
      "spark.app.name" -> name,

      "spark.driver.cores" -> s"${Math.round(driverCores)}",
      "spark.driver.memory" -> s"${Math.round(memoryPerCore * driverCores)}m",

      "spark.executor.cores" -> s"${Math.round(executorCores)}",
      "spark.executor.memory" -> s"${Math.round(memoryPerCore * executorCores)}m"
    )

    val spark = DefaultSparkSession(sparkConfig)

    println("Spark config: " + spark.conf.getAll.map(p => s"${p._1}: ${p._2}").mkString("{ ", ", ", " }"))

    spark
  }

}
