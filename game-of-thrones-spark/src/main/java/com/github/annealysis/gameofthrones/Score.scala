package com.github.annealysis.gameofthrones

import com.github.annealysis.gameofthrones.calculations.Calculations
import com.github.annealysis.gameofthrones.common.Spark
import com.github.annealysis.gameofthrones.io.{InputHandling, OutputHandling}
import com.typesafe.scalalogging.StrictLogging

class Score extends StrictLogging with Spark {

  import Score._

  def run(bucket: String, week: Int, createQuestionStructure: Boolean): Unit = {

    logger.info(s"This is episode ${week}.")

    logger.info("Reading in responses...")
    val (responsesDF, questionsDF) = InputHandling(s"$bucket/$responsesFile")

    responsesDF.show(5)
    questionsDF.show(5)

    if (createQuestionStructure) {
      logger.info("Writing answer structure file... ")
      InputHandling.writeAnswerStructure(questionsDF, s"$bucket/$answerStructureFile")
    }

    logger.info("Reading in correct answers...")
    // val correctAnswerDF = spark.read.csv(s"$bucket/$correctAnswersFile")
    val correctAnswerDF = InputHandling.readExcel(s"$bucket/$correctAnswersFile")

    logger.info("Scoring the responses... ")
    val scoredDF = Calculations(responsesDF, correctAnswerDF, s"$bucket/$rawResultsFile")

    logger.info("Combining previous weeks' scores, if applicable ... ")
    val combinedWeeksScoreDF = OutputHandling.combinePreviousScores(scoredDF, week, s"$bucket/$resultsFile")

    logger.info("Writing output to file... ")
    OutputHandling.writeScoresToFile(combinedWeeksScoreDF, s"$bucket/$resultsFile")

    logger.info("Done! ")

  }


}

object Score extends Score with App {

  val responsesFile = "fantasy_game_of_thrones_responses.csv"
  val correctAnswersFile = "correct_answer_testing.xlsx"
  //  val correctAnswersFile = "correct_answers.xlsx"

  val resultsFile = "results.csv"
  val rawResultsFile = "raw_results.csv"
  val answerStructureFile = "question_structure.csv"


  run(
    bucket = args(0),
    week = args(1).toInt,
    createQuestionStructure = args(2).toBoolean
  )


}