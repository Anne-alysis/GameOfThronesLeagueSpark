package com.github.annealysis.gameofthrones.common

import java.net.URI

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.SparkSession

/**
  * Allows all file names to be read in from a yaml configuration file.
  */
object Configuration {


  case class FileNames(
                        @JsonProperty("responseFile") responseFile: String,
                        @JsonProperty("answerStructureFile") answerStructureFile: String,
                        @JsonProperty("reshapedResponsesFile") reshapedResponsesFile: String,
                        @JsonProperty("correctAnswersFile") correctAnswersFile: String,
                        @JsonProperty("rawResultsFile") rawResultsFile: String,
                        @JsonProperty("resultsFile") resultsFile: String
                      )

  /** Retrieves the fully-qualified file names from GCS in a yaml file
    *
    * @param gcsBucket      : location of bucket in GCS
    * @param configFileName : YAML file name in GCS bucket
    * @param spark          : spark session
    * @return               : case class of file names with full GCS bucket prepended
    */
  def getFileNames(gcsBucket: String, configFileName: String)(implicit spark: SparkSession): FileNames = {

    implicit val hdfs: HdfsUtils = HdfsUtils(spark)
    val yamlURI = URI.create(s"$gcsBucket/$configFileName")

    val reader = hdfs.loadBytes(yamlURI)
    val mapper = new ObjectMapper(new YAMLFactory())
    val bareFileNames: FileNames = mapper.readValue(reader, classOf[FileNames])

    // prepend filenames with full path GCS bucket
    FileNames(
      s"$gcsBucket/${bareFileNames.responseFile}",
      s"$gcsBucket/${bareFileNames.answerStructureFile}",
      s"$gcsBucket/${bareFileNames.reshapedResponsesFile}",
      s"$gcsBucket/${bareFileNames.correctAnswersFile}",
      s"$gcsBucket/${bareFileNames.rawResultsFile}",
      bareFileNames.resultsFile
    )

  }

}
