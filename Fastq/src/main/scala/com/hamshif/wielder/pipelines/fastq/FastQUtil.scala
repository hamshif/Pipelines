package com.hamshif.wielder.pipelines.fastq

import com.hamshif.wielder.wild.{FsUtil}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * @author Gideon Bar
  *
  */
class FastQUtil extends FsUtil with FastQKeys with Logging {

  case class FastQFileName(sample: String, lane: String,read: String,end: String)


  val separateBarcodeUdf = udf(separateBarcode)

  def separateBarcode: (String => Array[String]) = {

    s => Array(s.substring(0, 12), s.substring(12, s.length))
  }


  val ramUniqueUdf = udf(ramUnique)

  def ramUnique: (String => String) = {

    s => {
      s.split(" ")(0)
    }
  }

  val fsFastqFilter: FileStatus => Boolean = file => file match {

    case f if f.isFile =>

      try {

        val name = f.getPath.toString.split("/").last

        name match {
          case a if a.endsWith(KEY_FASTQ) =>
            true
          case _ =>
            false
        }
      }
      catch {
        case e =>
          //          debug(s"couldn't parse $name")
          false
      }

    case _ =>
      false
  }


  def getFastFilesByLane(sparkSession: SparkSession, basePath: String, fs: FileSystem) = {

    println(s"Trying to read from:\n${basePath}")

    val sourcePath = new Path(basePath)

    val baseStatus = fs.getFileStatus(sourcePath)

    val fastqFiles = walkFiles(fs, baseStatus, fsFastqFilter)


    fastqFiles.foldLeft(Map[String, (String, String)]())((acc, status) => {

      println(s"fastq path: ${status.getPath}")

      val name = status.getPath.toString
      val splitName = name.split("/").last.split("_")

      val lane = splitName(2)

      val _read = splitName(3)

      var laneGroup = acc.getOrElse(lane, ("WTF", "WTF"))

      if(_read.contains("1")){
        laneGroup = (name, laneGroup._2)
      }
      else if(_read.contains("2")){
        laneGroup = (laneGroup._1, name)
      }

      acc + (lane -> laneGroup)
    })
  }


  def getSequenceDf(sparkSession: SparkSession, sc: SparkContext, sqlContext: SQLContext, path: String, isBarcodeSequence: Boolean): DataFrame = {

    import sqlContext.implicits._

    val raw = sparkSession.createDataset(sc.textFile(path).sliding(4, 4).map {
      case Array(sequence_identifier, sequence, quality_score_identifier, quality_score) =>
        (sequence_identifier, sequence, quality_score_identifier, quality_score)
    })

    isBarcodeSequence match {
      case true =>
        raw
          .toDF(BARCODE_DF_FIELDS:_*)
          .withColumn(KEY_S_UNIQUE, ramUniqueUdf(col(KEY_S_SEQUENCE_IDENTIFIER)))
      case false =>
        raw
          .toDF(SEQUENCE_DF_FIELDS:_*)
          .withColumn(KEY_UNIQUE, ramUniqueUdf(col(KEY_SEQUENCE_IDENTIFIER)))
    }
  }

  /**
    * Joins read1 and read2 fastq dataframes with the following extracted columns:
    * 1. Barcode Read UMI Extraction from read1.
    * 2. The first {minBases} extracted from read2 (can be used to determine if reads are similar)
    * 3. Accumulated read2 transcription fidelity quality score.
    * @param read1Df
    * @param read2Df
    * @param minBases
    * @return joind enriched dataframe
    */
  def joinFastqReadsWithFilteringExtractions(read1Df: DataFrame, read2Df: DataFrame, minBases: Int ): DataFrame = {

    read1Df
      .join(read2Df, read1Df(KEY_S_UNIQUE) === read2Df(KEY_UNIQUE),"inner")
      .drop(col(KEY_S_UNIQUE))
      .withColumn("_tmp", separateBarcodeUdf(col(KEY_S_SEQUENCE)))
      .withColumn(KEY_BARCODE, col("_tmp")(1))
      .withColumn(KEY_READ, col("_tmp")(0))
      .drop(col("_tmp"))
      .withColumn(KEY_MIN_READ, substring(col(KEY_SEQUENCE), 0, minBases))
      .withColumn(KEY_ACC_QUALITY_SCORE, accumulatedReadValueScoreUdf(col(KEY_QUALITY_SCORE)))
  }


  def toFastq(unitedReads: DataFrame, sinkDir: String, sampleName: String, fs: FileSystem, sc: SparkContext): Unit = {

    println(s"Sink directory:  $sinkDir")

    val targetPath = s"$sinkDir/$sampleName.$KEY_FASTQ"

    println(s"targetPath: $targetPath")

//    TODO add options for writing to multiple files by partitioning
//    val fastqFields = unitedReads
    unitedReads

      .select(KEY_SEQUENCE_IDENTIFIER, KEY_SEQUENCE, KEY_QUALITY_SCORE_IDENTIFIER, KEY_QUALITY_SCORE)
        .withColumn(
          KEY_FASTQ,
          toFastqStringUdf(col(KEY_SEQUENCE_IDENTIFIER), col(KEY_SEQUENCE), col(KEY_QUALITY_SCORE_IDENTIFIER), col(KEY_QUALITY_SCORE))
        )
      .select(KEY_FASTQ)

//    fastqFields
//      .show(1,false)
//
//
//    fastqFields
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
//      .partitionBy(KEY_FASTQ)
      .format("text")
      .save(sinkDir)

    val newPath = new Path(targetPath)
    fs.delete(newPath, true)

    val oldPath = fs.globStatus(new Path(s"$sinkDir/part*"))(0).getPath()

    fs.rename(oldPath, newPath)
  }


  val toFastqStringUdf = udf(toFastqString)

  def toFastqString: ((String, String, String, String) => String) = {

    (sequence_identifier, sequence, quality_score_identifier, quality) => {
      s"$sequence_identifier\n$sequence\n$quality_score_identifier\n$quality"
    }
  }


  val accumulatedReadValueScoreUdf = udf(accumulatedReadValueScore)

  def accumulatedReadValueScore: (String => Long) = {

    s => {
      val aa = s.foldLeft((IndexedSeq[Byte](), 0L))((acc, c) => {

        val l = c.toByte

//        print(s"${l.toLong} ")

        (acc._1 :+ l, acc._2 + l.toLong)
      })

//      println("")

      aa._2
    }
  }

  /**
    * Chooses the row with higher transcription quality
    * @param r1 Dataframe row
    * @param r2
    * @return The row with higher transcription quality
    */
  def byHigherTranscriptionQuality(r1: Row, r2: Row): Row = {

    val score1 = r1.getAs[Long](KEY_ACC_QUALITY_SCORE)
    val score2 = r2.getAs[Long](KEY_ACC_QUALITY_SCORE)

    score1 match {
      case s if s >= score2 =>
        r1
      case _ =>
        r2
    }
  }


}
