package com.github.annealysis.gameofthrones.io

import com.github.annealysis.gameofthrones.common.Utilities
import org.apache.spark.sql.functions.{col, concat, format_string, lit, split, trim}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object InputHandling {

  def apply(responsesFile: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    val initialDF = spark.read.option("header", "true").csv(responsesFile)
      .drop("Timestamp")

    val questionsDF = extractQuestions(initialDF)

    // rename columns and append question numbers with padding
    val renamedDF = renameColumns(initialDF)

    val colsToKeep = Seq("team", "payType")
    val colsToMelt = renamedDF.columns.filter(_.startsWith("Q"))
    val newCols = Seq("questionNumber", "answer")

    val meltDF = Utilities.melt(
      renamedDF,
      colsToKeep,
      colsToMelt,
      newCols
    )

    val finalDF = splitHybridQuestion(meltDF)

    (finalDF, questionsDF)

  }

  def extractQuestions(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val colNames: Array[String] = df.columns
    val questions = colNames.slice(4, colNames.length).map(_.trim)
    val questionsDF: DataFrame = questions.zipWithIndex.toSeq.toDF("questionFull", "id")
      .withColumn("questionNumber", concat(lit("Q"), format_string("%02d", $"id")))
      .drop("id")
      .withColumn("containsPerson", $"questionFull".contains("["))
      .withColumn("questionFragment", split($"questionFull", "\\(").getItem(0))

    val questionPointsDF = getPoints(questionsDF)

    val arrangedColNames = Seq("questionNumber", "questionFull", "question", "points")

    val nonpersonDF = questionPointsDF.filter(!$"containsPerson").drop("containsPerson")
      .withColumnRenamed("questionFragment", "question")
      .select(arrangedColNames.head, arrangedColNames.tail: _*)

    val personDF = questionPointsDF
      .filter($"containsPerson")
      .drop("containsPerson")
      .withColumn("person", concat(lit("["), split($"questionFull", "\\[").getItem(1)))
      .withColumn("question", concat($"questionFragment", lit(" "), $"person"))
      .select(arrangedColNames.head, arrangedColNames.tail: _*)

    nonpersonDF.union(personDF).orderBy("questionNumber")


  }

  def getPoints(df: DataFrame): DataFrame = {

    df
      .withColumn("_tmp",
        split(col("questionFull"), "\\(").getItem(1))
      .withColumn("points",
        split(col("_tmp"), " ").getItem(0).cast(IntegerType))
      .drop("_tmp")

  }


  def renameColumns(df: DataFrame): DataFrame = {

    val questionCount = df.columns.length - 4
    val numberedQuestions = (0 until questionCount).map("Q" + "0%1d".format(_))

    val newColNames: Seq[String] = Seq("name", "team", "payType", "splitType") ++ numberedQuestions

    df.toDF(newColNames: _*)

  }


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


  def writeAnswerStructure(df: DataFrame, bucketPath: String): Unit = {
    df.
      repartition(1)
      .orderBy("questionNumber")
      .write.csv(bucketPath)
  }


}
