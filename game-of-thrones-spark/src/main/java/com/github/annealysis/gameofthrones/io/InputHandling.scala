package com.github.annealysis.gameofthrones.io

import com.github.annealysis.gameofthrones.io.transformations.{Questions, Responses}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Handles reading in of each type of data.  Uses objects in
  * com.github.annealysis.gameofthrones.io.transformations to transform the raw responses and the questions */
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
    * *
    *
    * @param week                  : week/episode number
    * @param responseFile          : raw response file
    * @param answerStructureFile   : structure of answer sheet with no responses
    * @param reshapedResponsesFile : melted and cleaned raw response file
    * @param spark                 : implicit spark session created in Score.scala
    * @return melted and cleaned responses
    *
    */
  def apply(week: Int, responseFile: String, answerStructureFile: String, reshapedResponsesFile: String)(implicit spark: SparkSession): DataFrame = {

    // read in and return previously formed file, if not first week
    if (week > 1) {
      logger.info("Reading in previously reshaped response file... ")
      return spark.read.option("header", "true").csv(reshapedResponsesFile)
    }

    val rawResponsesDF = spark.read.option("header", "true").csv(responseFile).drop("Timestamp")

    Questions(rawResponsesDF, answerStructureFile)

    Responses(rawResponsesDF, reshapedResponsesFile)

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
