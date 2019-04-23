package com.github.annealysis.gameofthrones.io

import com.github.annealysis.gameofthrones.common.Utilities
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.{col, concat, format_string, lit, split, trim}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Handles reading in of each type of data */
object InputHandling extends StrictLogging {


  /** Returns cleaned responses from raw responses.
    *
    * Note: behavior differs depending on value of `week`
    *
    * Week 1:
    * For the teams' responses, actual question text is scrubbed, and questions are separated into their own DataFrame,
    * `questionsDF`.  Instead of the raw text in the response DataFrame, for clarity the question identifiers are
    * converted into "QXX", where "XX" is the question number.  Each question is a column in the raw import,
    * so the questions are melted so that each team/question combination comprises its own row.  The output is written
    * to a file so these steps can be circumvented in subsequent weeks.
    *
    * The `questionsDF` is cleaned to extract the point value to its own column and to remove the point values from
    * the question text.  This cleaned structure is written to an output file as a template to update correct answers
    * week over week.
    *
    * Week > 1:
    * Simply reads in the cleaned data that has been scrubed and saved in week 1.
    *
    * @param week           : week/episode number
    * @param responsesFiles : sequence of input file paths
    * @param spark          : implicit spark session created in Score.scala
    * @return melted and cleaned responses
    *
    */
  def apply(week: Int, responsesFiles: Seq[String])(implicit spark: SparkSession): DataFrame = {

    // read in previously formed file, if not first week
    if (week > 1) {
      logger.info("Reading in previously reshaped response file... ")
      return spark.read.option("header", "true").csv(responsesFiles(2))
    }

    //otherwise, reshape the responses
    val initialDF = spark.read.option("header", "true").csv(responsesFiles(0)).drop("Timestamp")

    val questionsDF = extractQuestions(initialDF)
    writeAnswerStructure(questionsDF, responsesFiles(1))

    // rename columns and append question numbers with padding
    val renamedDF = renameColumns(initialDF)

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
      .csv(responsesFiles(2))

    finalResponseDF

  }


  /** returns a cleaned and bare DataFrame comprised only of questions and their properties.
    *
    * Some questions contain a person listed after the point values in square brackets.  After points are extracted
    * via the `getPoints` method, this person text is added back into the question.
    *
    * e.g., "Does Arya personally kill the following characters on her list (1 point each) [Cersei Lannister]"
    * becomes "Does Arya personally kill the following characters on her list [Cersei Lannister]"
    *
    * @param df    : the initial raw response DataFrame with questions to be extracted
    * @param spark : implicit spark session created in Score.scala
    * @returns DataFrame of questions only, including columns for points and question numbers
    */
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
      .withColumn("person",
        concat(lit("["), split($"questionFull", "\\[").getItem(1)))
      .withColumn("question",
        concat($"questionFragment", lit(" "), $"person"))
      .select(arrangedColNames.head, arrangedColNames.tail: _*)

    nonpersonDF.union(personDF).orderBy("questionNumber")


  }


  /** returns a column of point values per question
    *
    * @param df : a dataframe with questions as a single column, including the point value
    * @returns the point values as a separate column
    */
  def getPoints(df: DataFrame): DataFrame = {

    df
      .withColumn("_tmp",
        split(col("questionFull"), "\\(").getItem(1))
      .withColumn("points",
        split(col("_tmp"), " ").getItem(0).cast(IntegerType))
      .drop("_tmp")

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


  /** writes the cleaned questions to a file for ease of updating correct answers week by week
    *
    * @param df         : cleaned question DataFrame
    * @param bucketPath : GCS bucket path to write the output to
    */
  def writeAnswerStructure(df: DataFrame, bucketPath: String): Unit = {
    df.
      repartition(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(bucketPath)
  }


  /** returns correct answer dataframe, read from Excel file on GCS
    *
    * @param fileName : GCS bucket path for excel file
    * @param spark    : implicit spark session created in Score.scala
    * @return DataFrame of correct answers per question
    */
  def readExcel(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      // .option("dataAddress", sheet) // Required
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", "False")
      .load(fileName)
  }

}
