package com.github.annealysis.gameofthrones

import com.github.annealysis.gameofthrones.calculations.Calculations
import com.github.annealysis.gameofthrones.common.Spark
import com.github.annealysis.gameofthrones.io.{InputHandling, OutputHandling}
import com.typesafe.scalalogging.StrictLogging


/** Main class that performs all actions to score a week's results. */
class Score extends StrictLogging with Spark {

  import Score._

  /** Runs each step of the code, reading in data, processing it, and writing it out.
    *
    * @param bucket : primary GCS path that stores all files
    * @param week   : week/episode number
    */
  def run(bucket: String, week: Int): Unit = {

    logger.info(s"This is episode ${week}.")

    logger.info("Reading in responses...")
    val inputFilesWithBucket = inputFiles.map(i => s"$bucket/${i}")
    val responsesDF = InputHandling(week, inputFilesWithBucket)

    logger.info("Reading in correct answers...")
    val correctAnswerDF = InputHandling.readExcel(s"$bucket/$correctAnswersFile")

    logger.info("Scoring the responses... ")
    val scoredDF = Calculations(responsesDF, correctAnswerDF, s"$bucket/archive/$rawResultsFile")

    logger.info("Combining previous weeks' scores, if applicable ... ")
    val combinedWeeksScoreDF = OutputHandling.combinePreviousScores(scoredDF, week, bucket, resultsFile)

    logger.info("Writing output to file... ")
    OutputHandling.writeScoresToFile(combinedWeeksScoreDF, s"$bucket/$resultsFile")

    logger.info("Done! ")

  }

}


/** Companion object to Score.scala that stores file names and triggers the `run` method.  */
object Score extends Score with App {

  val responsesFile = "fantasy_game_of_thrones_responses.csv" // raw download of team responses from Google form
  val answerStructureFile = "question_structure.csv" // file to write the structure of the questions
  val reshapedResponsesFile = "reshaped_responses.csv" // file saved during week 1, to be read in during subsequent weeks
  val inputFiles = Seq(responsesFile, answerStructureFile, reshapedResponsesFile)

  // file updated week-by-week with new correct answers, whose structure is generated from `answerStructureFile`
  val correctAnswersFile = "correct_answers.xlsx"

  val resultsFile = "results.csv" // scores and ranks aggregated by team
  val rawResultsFile = "raw_results.csv" // unaggregated scores

  run(
    bucket = args(0),
    week = args(1).toInt
  )

}