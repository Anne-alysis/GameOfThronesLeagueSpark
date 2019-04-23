package com.github.annealysis.gameofthrones.calculations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, rank, sum, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Calculates scores and ranks per team */
object Calculations {

  /** Creates a score and rank for each team, given a week's correct answers
    *
    * @param responsesDF     : cleaned response DataFrame from the Google form for each team
    * @param correctAnswerDF : correct answer DataFrame, including point values
    * @param bucketPath      : the GCS bucket path to write the scored questions, before aggregating
    * @param spark           : implicit spark session created in Score.scala
    * @return aggregated DataFrame of scores/ranks for each team
    */
  def apply(responsesDF: DataFrame, correctAnswerDF: DataFrame, bucketPath: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val mergedDF: DataFrame = responsesDF.join(correctAnswerDF, Seq("questionNumber"), "left")
      .withColumn("correct", checkCorrectUDF($"include", $"multipleAnswers", $"answer", $"correctAnswer"))
      .withColumn("score", $"correct".cast(IntegerType) * $"points")

    mergedDF.repartition(1).write.mode("overwrite").option("header", "true").csv(s"$bucketPath")

    val aggregateDF: DataFrame = aggregateResults(mergedDF)

    aggregateDF
  }

  //todo: check scaladoc for UDF
  /** UDF to check if a given response is correct
    *
    * @param include         : boolean for if the question should be scored
    * @param multipleAnswers : boolean for if the question potentially has more than one correct answer
    * @param answer          : team's response to question
    * @param correctAnswer   : correct answer
    * @return true if answer is correct
    */
  val checkCorrectUDF = udf((include: Boolean, multipleAnswers: Boolean, answer: String, correctAnswer: String) => {

    if (!include) {
      false
    } else {
      if (multipleAnswers) {
        // remove all punctuation and make all letters lower case
        val answerMunged = mungeAnswers(answer)
        val correctAnswerMunged = mungeAnswers(correctAnswer)

        correctAnswerMunged.contains(answerMunged)

      } else {
        answer == correctAnswer
      }
    }
  }
  )


  /** helper function for checkCorrectUDF to clean string by lowering letters and removing punctuation
    *
    * @param answer : uncleaned response for a team
    * @return cleaned response
    */
  def mungeAnswers(answer: String): String = {
    answer
      .toLowerCase
      .replaceAll("""[\p{Punct}]""", "")
  }


  /** aggregates responses to the team level and ranks teams
    *
    * @param df : DataFrame with a score for each question and team
    * @return DataFrame with score summed and ranked for each team
    */
  def aggregateResults(df: DataFrame): DataFrame = {

    val w = Window.orderBy(col("score").desc)

    df.groupBy("team", "payType")
      .agg(sum("score").as("score"))
      .orderBy(desc("score"))
      .withColumn("rank", rank.over(w))
      .select("team", "payType", "rank", "score")


  }
}
