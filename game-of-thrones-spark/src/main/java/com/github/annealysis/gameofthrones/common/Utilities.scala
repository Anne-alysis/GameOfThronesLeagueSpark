package com.github.annealysis.gameofthrones.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object Utilities {

  // assumes multiple types of columns with “_X” denoting the company number
  def melt(df: DataFrame, colsToKeep: Seq[String], colsToMelt: Seq[String], newNames: Seq[String]): DataFrame = {

    val n = colsToMelt.indices
    val newNamesFull = colsToKeep ++ newNames.reverse

    n.map(i => {
      val colsToInclude = colsToKeep :+ colsToMelt(i)
      df.select(colsToInclude.head, colsToInclude.tail: _*)
        .withColumn("temp", lit(colsToMelt(i)))

    }).reduce(_ union _).toDF(newNamesFull: _*)

  }

}

trait Spark {
  implicit val spark = SparkUtils.createSparkSession(s"${this.getClass.getSimpleName}")
}