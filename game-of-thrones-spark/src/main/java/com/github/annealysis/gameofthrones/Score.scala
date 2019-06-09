package com.github.annealysis.gameofthrones

import com.github.annealysis.gameofthrones.calculations.Calculations
import com.github.annealysis.gameofthrones.common.Configuration.FileNames
import com.github.annealysis.gameofthrones.common.{Configuration, Spark}
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

    logger.info(s"This is episode $week.")

    logger.info("Reading in configuration file ...")
    val fileNames: FileNames = Configuration.getFileNames(bucket, yamlFileName)

    logger.info("Reading in responses...")
    val responsesDF = InputHandling(week, fileNames.responseFile, fileNames.answerStructureFile, fileNames.reshapedResponsesFile)

    logger.info("Reading in correct answers...")
    val correctAnswerDF = InputHandling.readExcel(fileNames.correctAnswersFile)

    logger.info("Scoring the responses... ")
    val scoredDF = Calculations(responsesDF, correctAnswerDF, fileNames.rawResultsFile)

    logger.info("Combining previous weeks' scores, if applicable ... ")
    val combinedWeeksScoreDF = OutputHandling.combinePreviousScores(scoredDF, week, bucket, fileNames.resultsFile)

    logger.info("Writing output to file... ")
    OutputHandling.writeScoresToFile(combinedWeeksScoreDF, s"$bucket/${fileNames.resultsFile}")

    logger.info("Done! ")

  }

}


/** Companion object to Score.scala that stores file names and triggers the `run` method.  */
object Score extends Score with App {

  val yamlFileName = "got.yaml"

  run(
    bucket = args(0),
    week = args(1).toInt
  )

}