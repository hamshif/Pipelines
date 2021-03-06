package com.hamshif.wielder.pipelines.fastq

import com.hamshif.wielder.wild.FsUtil
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

import scala.collection.mutable

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


  def getFastqFilesByLane(sparkSession: SparkSession, basePath: String, fs: FileSystem) = {

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
      .withColumn(KEY_UMI, col("_tmp")(1))
      .withColumn(KEY_BARCODE, col("_tmp")(0))
      .drop(col("_tmp"))
      .withColumn(KEY_MIN_READ, substring(col(KEY_SEQUENCE), 0, minBases))
      .withColumn(KEY_MIN_READ_BARCODE, concat(col(KEY_MIN_READ), lit("_"), col(KEY_S_SEQUENCE)))
      .withColumn(KEY_ACC_QUALITY_SCORE, accumulatedReadValueScoreUdf(col(KEY_QUALITY_SCORE)))

  }


  def toFastq(fastqDf: DataFrame, sinkDir: String, sampleName: String, fs: FileSystem, sc: SparkContext): Unit = {

    println(s"Sink directory:  $sinkDir")

    val partsPath = s"$sinkDir/parts"
    println(s"Writing partitioned files to:  $partsPath")

    fastqDf

      .select(KEY_SEQUENCE_IDENTIFIER, KEY_SEQUENCE, KEY_QUALITY_SCORE_IDENTIFIER, KEY_QUALITY_SCORE)
        .withColumn(
          KEY_FASTQ,
          toFastqStringUdf(col(KEY_SEQUENCE_IDENTIFIER), col(KEY_SEQUENCE), col(KEY_QUALITY_SCORE_IDENTIFIER), col(KEY_QUALITY_SCORE))
        )
      .select(KEY_FASTQ)
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save(partsPath)

    val mergedFullPath = s"$sinkDir/merged/$sampleName.$KEY_FASTQ"

    println(s"Wrote files per partitions\nGoing to merge them to:\n$mergedFullPath")

    val mergedPath = new Path(mergedFullPath)
    fs.delete(mergedPath, true)
    FileUtil.copyMerge(fs, new Path(partsPath), fs, mergedPath, false, fs.getConf, null)
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
    * @return The row with higher transcription quality
    */
  def byHigherTranscriptionQuality: ((Row, Row) => Row) = {

    (r1, r2) => {

      //    val j1 = r1.getAs[String](KEY_MIN_READ)
      //    val j2 = r2.getAs[String](KEY_MIN_READ)
      //
      ////    println(s"value1: ${j1} value2: ${j2}")

      val score1 = r1.getAs[Long](KEY_ACC_QUALITY_SCORE)
      val score2 = r2.getAs[Long](KEY_ACC_QUALITY_SCORE)

      val r3 = score1 match {
        case s if s >= score2 =>

          r1
        case _ =>
          r2
      }

      val acc1 = r1.getAs[Long](KEY_FILTERED_SIMILAR)
      val acc2 = r2.getAs[Long](KEY_FILTERED_SIMILAR)

      val acc = acc1 + acc2 + 1L

      val r = updateRow(r3, 15, acc)

      r
    }
  }


  /**
    * Chooses the row with higher transcription quality
    * @return The row with higher transcription quality
    */
  def byHigherTranscriptionQuality1: ((Row, Row) => Row) = {

    (r1, r2) => {

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

  /**
    * Updates a row with a schema
    * note that if the type is incorrect you will get an exception
    *
    * @param r a row
    * @param i the index to update
    * @param a the value to update
    * @return the updated row
    */
  def updateRow(r: Row, i: Int, a: Any): Row = {

    val s: Array[Any] = r
      .toSeq
      .toArray
      .updated(i, a)

    new GenericRowWithSchema(s, r.schema)
  }


  def limitSize(n: Int, arrCol: Column): Column = {
    array( (0 until n).map( arrCol.getItem ): _* )
  }


  def red(n: Column, arrCol: Column): Column = {

    val size = n.cast(Long.getClass.getTypeName)

    val t = (0 until 2)
      .map(i => {
        arrCol.getItem(i)
      })
      .reduce((c1, c2) => {

        c1
      })

    arrCol
  }


  def arrayHead = (x: Any) => {

    val r = x.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]

    val best = r.head

    best
  }


  def reduceByQuality= (x: Any) => {

    val d = x.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]

    val red = d.reduce((r1, r2) => {

      val quality1 = r1.getAs[Long](KEY_ACC_QUALITY_SCORE)
      val quality2 = r2.getAs[Long](KEY_ACC_QUALITY_SCORE)

      val r3 = quality1 match {
        case a if a >= quality2 =>
          r1
        case _ =>
          r2
      }

      r3
    })

    red
  }


}
