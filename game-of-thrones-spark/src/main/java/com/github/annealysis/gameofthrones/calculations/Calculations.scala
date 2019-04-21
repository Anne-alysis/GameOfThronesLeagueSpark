package com.github.annealysis.gameofthrones.calculations

import org.apache.spark.sql.{DataFrame, SparkSession}

object Calculations {

  def apply(responsesDF: DataFrame, correctAnswerDF: DataFrame)(implicit spark: SparkSession): DataFrame = {


    responsesDF
  }


}
