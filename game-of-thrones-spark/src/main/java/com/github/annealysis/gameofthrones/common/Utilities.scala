package com.github.annealysis.gameofthrones.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/** Helper methods used throughout repo */
object Utilities {

  /** Returns a long, narrow (melted) DataFrame from a wide DataFrame
    *
    * @param df         : initial wide data to melt
    * @param colsToKeep : sequence of columns to hold fixed per row
    * @param colsToMelt : sequence of columns to melt
    * @param newNames   : sequence of new names for melted columns (e.g., "Type" and "Value", where "Type" is the
    *                   value of the initial column, and "Value" is the value of the cell)
    * @return melted version of the DataFrame
    */
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


/** Creates an implicit spark session to be used throughout the App */
trait Spark {
  implicit val spark = SparkUtils.createSparkSession(s"${this.getClass.getSimpleName}")
}