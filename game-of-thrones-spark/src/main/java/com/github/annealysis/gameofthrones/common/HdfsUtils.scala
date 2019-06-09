package com.github.annealysis.gameofthrones.common

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Allows interaction with Hadoop file system on GCS.
  *
  * Note: this is NOT my code but borrowed from Dane G so the program can run.
  */

class HdfsUtils(hadoopConfiguration: Configuration) {

  def getFilesystem(uri: URI): FileSystem = {
    FileSystem.get(uri, hadoopConfiguration)
  }

  def path(uri: URI): Path = {
    new Path(uri.toString)
  }

  def load[T](f: InputStream => T, uri: URI): T = {
    val hdfs = getFilesystem(uri)
    val inputStream: InputStream = hdfs.open(path(uri))

    try {
      f.apply(inputStream)
    } finally {
      inputStream.close()
    }
  }

  def loadBytes(uri: URI): Array[Byte] = {
    load(is => {
      val os = new ByteArrayOutputStream()
      IOUtils.copyBytes(is, os, 1024)
      os.toByteArray
    }, uri)
  }

}

object HdfsUtils {
  def apply(hadoopConfiguration: Configuration): HdfsUtils = new HdfsUtils(hadoopConfiguration)
  def apply(sc: SparkContext): HdfsUtils = HdfsUtils(sc.hadoopConfiguration)
  def apply(spark: SparkSession): HdfsUtils = HdfsUtils(spark.sparkContext.hadoopConfiguration)
}

