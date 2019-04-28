package com.github.annealysis.gameofthrones.io.transformations

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.{col, concat, format_string, lit, split}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** This object transforms the questions. */
object Questions extends StrictLogging {


  /** returns a cleaned and bare DataFrame comprised only of questions and their properties.
    *
    * Some questions contain a character listed after the point values in square brackets.  After points are extracted
    * via the `getPoints` method, this character text is added back into the question with `rearrangeCharacterName`
    *
    * @param df       : the initial raw response DataFrame with questions to be extracted
    * @param fileName : the location to write the structure of the questions
    * @param spark    : implicit spark session created in Score.scala
    * @return
    */
  def apply(df: DataFrame, fileName: String)(implicit spark: SparkSession): Unit = {

    val rawQuestionsDF = getQuestionsFromRawResponses(df)

    val questionPointsDF = getPoints(rawQuestionsDF)

    val arrangedColNames = Seq("questionNumber", "questionFull", "question", "points")
    val questionsDF = rearrangeCharacterName(questionPointsDF, arrangedColNames)

    writeAnswerStructure(questionsDF, fileName)

  }

  /** Extracts the question text from the raw response csv.  Adds helper columns, including question numbers and
    * if the question contains a character's name to be rearranged in `rearrangeCharacterName`.
    *
    * @param df    : raw response csv
    * @param spark : implicit spark session created in Score.scala
    * @return
    */
  def getQuestionsFromRawResponses(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val colNames: Array[String] = df.columns
    val questions = colNames.slice(4, colNames.length).map(_.trim)

    questions.zipWithIndex.toSeq.toDF("questionFull", "id")
      .withColumn("questionNumber", concat(lit("Q"), format_string("%02d", $"id")))
      .drop("id")
      .withColumn("containsCharacter", $"questionFull".contains("["))
      .withColumn("questionFragment", split($"questionFull", "\\(").getItem(0))

  }


  /** returns a column of point values per question
    *
    * @param df : a dataframe with questions as a single column, including the point value
    * @return the point values as a separate column
    */
  def getPoints(df: DataFrame): DataFrame = {

    df
      .withColumn("_tmp",
        split(col("questionFull"), "\\(").getItem(1))
      .withColumn("points",
        split(col("_tmp"), " ").getItem(0).cast(IntegerType))
      .drop("_tmp")

  }


  /** Moves the characters name from the end of the original raw question to after the question fragment (i.e.,
    * the points parenthetical is removed)
    *
    * e.g., "Does Arya personally kill the following characters on her list (1 point each) [Cersei Lannister]"
    * becomes "Does Arya personally kill the following characters on her list [Cersei Lannister]"
    *
    * @param df               : bare question dataframe with points extracted to column previously
    * @param arrangedColNames : new order of columns in DF
    * @param spark            : implicit spark session created in Score.scala
    * @return a dataframe with the characters name placed next to the question fragment extracted in
    *         getQuestionsFromRawResponses
    */
  def rearrangeCharacterName(df: DataFrame, arrangedColNames: Seq[String])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val nonCharacterDF = df.filter(!$"containsCharacter").drop("containsCharacter")
      .withColumnRenamed("questionFragment", "question")
      .select(arrangedColNames.head, arrangedColNames.tail: _*)

    val characterDF = df
      .filter($"containsCharacter")
      .drop("containsCharacter")
      .withColumn("character",
        concat(lit("["), split($"questionFull", "\\[").getItem(1)))
      .withColumn("question",
        concat($"questionFragment", lit(" "), $"character"))
      .select(arrangedColNames.head, arrangedColNames.tail: _*)

    nonCharacterDF.union(characterDF).orderBy("questionNumber")

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


}
