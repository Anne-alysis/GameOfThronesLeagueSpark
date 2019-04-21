package com.github.annealysis.gameofthrones.io

import org.apache.spark.sql.{DataFrame, SparkSession}

object OutputHandling {

  def combinePreviousScores(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    df
  }

  def writeScoresToFile(df: DataFrame)(implicit spark: SparkSession): Unit = {



  }

}
