package com.github.annealysis.gameofthrones.common

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Generates a default SparkSession with sane logging and appropriate performance settings for a local
  * emulation vs. being run as a job on a Spark server
  *
  * Note: this is NOT my code but borrowed from Dane G so the program can run.
  */

object DefaultSparkSession {

  lazy val logger: Logger = org.apache.log4j.Logger.getLogger(this.getClass)

  lazy val isStandalone: Boolean = Option(System.getenv("SPARK_ENV_LOADED")).isEmpty

  lazy val hasYarn: Boolean = Option(System.getenv("DATAPROC_MASTER_COMPONENTS")).exists(_.contains("hadoop-yarn-resourcemanager"))

  lazy val hasHive: Boolean = Option(System.getenv("DATAPROC_COMMON_COMPONENTS")).exists(_.contains("hive"))


  def suppressLogging(): Unit = {
    Array(
      "org.apache.hadoop",
      "org.apache.parquet",
      "org.spark_project.jetty",
      "org.apache.spark.scheduler",
      "org.apache.spark.storage",
      "org.apache.spark.sql",
      "org.apache.spark.executor",
      "org.apache.spark.mapred",
      "org.apache.spark.ContextCleaner",
      "org.apache.spark.SparkContext"
    ).foreach(org.apache.log4j.Logger.getLogger(_).setLevel(org.apache.log4j.Level.WARN))
  }

  def apply(config: Map[String, String] = Map()): SparkSession = {
    suppressLogging()

    val master = if(hasYarn) "yarn" else "local[*]"

    val sessionBuilder = SparkSession.builder.master(master)

    val baseConfig = new SparkConf()
      .set("spark.ui.enabled", false.toString) // we don't need no stinking UI
      .set("spark.ui.showConsoleProgress", false.toString)
      .set("spark.dynamicAllocation.enabled", true.toString)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if (isStandalone) {
      logger.info(s"creating default Spark session in standalone mode using master='$master'")

      sessionBuilder.config(
        baseConfig
          .set("spark.driver.bindAddress", "127.0.0.1") // force use of loopback device for local
          .set("spark.driver.host", "127.0.0.1")
          .setAll(config)
      )

    } else {

      logger.info(s"creating default Spark session in cluster mode using master='$master'")

      sessionBuilder.config(baseConfig.setAll(config))

      if(hasYarn) {
        logger.info(s"configuring dynamic executor allocation")
        sessionBuilder.config("spark.dynamicAllocation.enabled", true.toString)
        sessionBuilder.config("spark.shuffle.service.enabled", true.toString)
        sessionBuilder.config("spark.yarn.shuffle.stopOnFailure", false.toString)
      }

    }

    sessionBuilder.getOrCreate()
  }

}
