package com.github.annealysis.gameofthrones.io.transformations

import com.github.annealysis.gameofthrones.common.Utilities
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.{split, trim}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Transforms the raw responses */
object Responses extends StrictLogging {


  /** This object renames columns, melts the wide data, splits question 26 into two rows from one, and
    * writes the output to a file
    *
    * @param df       : raw response file
    * @param fileName : name of file to write the reshaped results to
    * @param spark    : implicit spark session created in Score.scala
    * @return a cleaned, reshaped response file with one row per question/team combination
    */
  def apply(df: DataFrame, fileName: String)(implicit spark: SparkSession): DataFrame = {
    // rename columns and append question numbers with padding
    val renamedDF = renameColumns(df)

    val colsToKeep = Seq("team", "payType")
    val colsToMelt = renamedDF.columns.filter(_.startsWith("Q"))
    val newCols = Seq("questionNumber", "answer")

    val meltDF = Utilities.melt(renamedDF, colsToKeep, colsToMelt, newCols)

    val finalResponseDF = splitHybridQuestion(meltDF)

    // write out restructured file for reading in subsequent weeks
    finalResponseDF.repartition(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(fileName)

    finalResponseDF

  }


  /** returns a renamed dataframe with full text questions renamed as "QXX", where "XX" is the (potentially padded)
    * number (e.g., "01", "13")
    *
    * @param df : raw response dataframe
    * @return dataframe with question columns renamed
    */
  def renameColumns(df: DataFrame): DataFrame = {

    val questionCount = df.columns.length - 4
    val numberedQuestions = (0 until questionCount).map("Q" + "%02d".format(_))

    val newColNames: Seq[String] = Seq("name", "team", "payType", "splitType") ++ numberedQuestions

    df.toDF(newColNames: _*)

  }


  /** returns a dataframe with question 26 split into two rows per team
    *
    * Question 26 allows for a team to write in two responses in the format: "X kills Y. Z kills K."  This is
    * difficult to score, given the scoring UDF, so the responses is split into two rows: "X kills Y" and "Z kills K"
    * for each row.
    *
    * @param df    : melted, mostly cleaned responses data frame
    * @param spark : implicit spark session created in Score.scala
    * @return DataFrame with multiple rows per team for question 26.
    */
  def splitHybridQuestion(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val keepDF = df.filter($"questionNumber" =!= "Q26")
    val toMungeDF = df.filter($"questionNumber" === "Q26")

    val splitDF = {
      toMungeDF.withColumn("_tmp", split($"answer", "\\."))
        .withColumn("answer1", trim($"_tmp".getItem(0)))
        .withColumn("answer2", trim($"_tmp".getItem(1)))
        .drop("_tmp")
    }

    val originalNames = df.columns
    val colNames: Array[String] = originalNames.filterNot(_ == "answer")
    val colNames1 = colNames :+ "answer1"
    val colNames2 = colNames :+ "answer2"

    val reshapedDF = splitDF.select(colNames1.head, colNames1.tail: _*)
      .union(
        splitDF.select(colNames2.head, colNames2.tail: _*)
      )
      .withColumnRenamed("answer1", "answer")
      .select(originalNames.head, originalNames.tail: _*)

    keepDF.union(reshapedDF)

  }

}
