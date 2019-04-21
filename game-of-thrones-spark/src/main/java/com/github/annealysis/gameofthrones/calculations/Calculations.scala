package com.github.annealysis.gameofthrones.calculations

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, rank, sum, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Calculations {

  def apply(responsesDF: DataFrame, correctAnswerDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val mergedDF = responsesDF.join(correctAnswerDF, Seq("questionNumber"), "left")
      .withColumn("correct", checkCorrectUDF($"include", $"multipleAnswers", $"answer", $"correctAnswer"))
      .withColumn("score", $"correct".cast(IntegerType) * $"points")

    //mergedDF.repartition(1).write.mode("overwrite").csv(s"$bucket/raw_results.csv")

    val aggregateDF = aggregateResults(mergedDF)

    aggregateDF
  }

  val checkCorrectUDF = udf((include: Boolean, multipleAnswers: Boolean, answer: String, correctAnswer: String) => {

    if (!include) {
      null
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


  def mungeAnswers(answer: String): String = {
    answer
      .toLowerCase
      .replaceAll("""[\p{Punct}]""", "")
  }


  def aggregateResults(df: DataFrame): DataFrame = {


    val w = Window.orderBy(col("score").desc)

    df.groupBy("team", "payType")
      .agg(sum("score").as("score"))
      .orderBy(desc("score"))
      .withColumn("rank", rank.over(w))


  }
}
