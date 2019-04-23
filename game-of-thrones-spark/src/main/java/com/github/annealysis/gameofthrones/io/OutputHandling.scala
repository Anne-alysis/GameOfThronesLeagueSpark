package com.github.annealysis.gameofthrones.io

import java.time.{LocalDate, ZoneId}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/** Handles the data to output to file by combining with previous weeks' scores, if applicable */
object OutputHandling {


  /** returns a cleaned output DataFrame to write to a file.
    *
    * @param df       : scored and ranked DataFrame for the week, aggregated by team
    * @param week     : episode/week number
    * @param bucket   : GCS path to read previous weeks' results, if week > 1
    * @param fileName : name of results file
    * @param spark    : implicit spark session created in Score.scala
    * @return scores and ranks of this week's episode and previous weeks' episodes (if week > 1)
    */
  def combinePreviousScores(df: DataFrame, week: Int, bucket: String, fileName: String)(implicit spark: SparkSession): DataFrame = {

    val now = LocalDate.now(ZoneId.of("America/Los_Angeles"))

    // rename columns to be more aesthetically pleasing for final results
    val renamedDF = df.withColumnRenamed("score", s"Episode $week Score")
      .withColumnRenamed("rank", s"Episode $week Rank")
      .withColumnRenamed("team", "Team")
      .withColumnRenamed("payType", "Iron Bank")

    if (week == 1) return renamedDF

    val oldResultsDF = spark.read.option("header", "true").csv(s"$bucket/$fileName")

    // save results for posterity
    val resultsFileSplit = fileName.split("\\.")(0)
    oldResultsDF.repartition(1).write
      .mode("overwrite").option("header", "true").csv(s"$bucket/archive/${resultsFileSplit}_$now.csv")

    // include movement of rank from week to week (negative because lower rank is better
    val movementName = "Movement from Previous Episode"
    val oldResultsDroppedDF = oldResultsDF.drop(movementName)
    val combinedResultsDF = renamedDF.join(oldResultsDroppedDF, Seq("Team", "Iron Bank"), "left")
      .withColumn(movementName, -(col(s"Episode $week Rank") - col(s"Episode ${week - 1} Rank")))

    // rearrange movement column to be in the middle, not at the end
    val colNames = combinedResultsDF.columns
    val firstHalfNames = colNames.slice(0, 3) :+ movementName
    val newNames = firstHalfNames ++ colNames.slice(3, colNames.length - 1)

    combinedResultsDF.select(newNames.head, newNames.tail: _*)

  }


  /** writes scored and ranked output to GCS
    *
    * @param df         : scored and ranked data
    * @param bucketPath : path on GCS to write to
    * @param spark      : implicit spark session created in Score.scala
    */
  def writeScoresToFile(df: DataFrame, bucketPath: String)(implicit spark: SparkSession): Unit = {
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(bucketPath)
  }

}
