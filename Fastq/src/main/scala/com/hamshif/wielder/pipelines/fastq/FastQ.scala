package com.hamshif.wielder.pipelines.fastq

import com.hamshif.wielder.wild.{DatalakeConfig, FsUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * @author Gideon Bar
  * An ETL for ingesting and filtering FASTQ format genetic sequencing text files.
  * As of now it pairs fastq files to Read 1 and Read 2, "Joins" them into 1 dataframe.
  * It then sequentially combines all paired dataframes into 1
  * and filters for duplicates, outputting a cardinal account of the filtering stages.
  */
object FastQ extends FastQUtil with FastqArgParser with FsUtil with FastQKeys with Logging {

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)
    val fastqConf = getSpecificConf(args)

    FastQ.start(conf, fastqConf)
  }


  def start(conf: DatalakeConfig, fastqConf: FastqConfig): Unit = {


    val fastqDir = f"${conf.bucketName}"

    val basePath = s"${conf.fsPrefix}${fastqDir}"

    val sinkDir = s"$basePath-filtered"

    val sparkSession = SparkSession
      .builder()
      .appName(FastQ.getClass.getName)
      .master(conf.sparkMaster)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val sqlContext = sparkSession.sqlContext


    val fs = new Path(basePath).getFileSystem(sc.hadoopConfiguration)

    val byLane = getFastFilesByLane(sparkSession, basePath, fs)

    val sampleName = byLane.head._2._1
      .split("/").last
      .split("_").head


    val lanes = byLane.foldLeft((List[(DataFrame, Long)]()))((acc, laneTuple) => {

      val t1 = laneTuple._2._1.replace("file:", "")
      val t2 = laneTuple._2._2.replace("file:", "")

      val raw1 = getSequenceDf(sparkSession, sc, sqlContext, t1, true)

      val raw2: DataFrame = getSequenceDf(sparkSession, sc, sqlContext, t2, false)


      raw1.show(30, false)

      raw2.show(30, false)


      val rawBarcodeDataCardinality = raw1.count()
      val rawSequenceTailDataCardinality = raw2.count()
      //
      ////
      //    raw1.printSchema()
      //
      //    raw2.printSchema()

      raw1.unpersist(true)
      raw2.unpersist(true)

      val joinedWithBarcode = joinWithBarcodeReadExtraction(raw1, raw2)

      val barcodeSeparatedCardinality = joinedWithBarcode.count()

      println(s"Joined data from\n$t1\n$t2\n")
      println(s"$t1 size is:                  $rawBarcodeDataCardinality")
      println(s"$t2 size is:                  $rawSequenceTailDataCardinality")
      println(s"barcodeSeparatedCardinality:  $barcodeSeparatedCardinality")

      joinedWithBarcode
        .select(col(KEY_UNIQUE), col(KEY_S_SEQUENCE), col(KEY_SEQUENCE))
        .show(false)

      (joinedWithBarcode, barcodeSeparatedCardinality) :: acc
    })

    println("Starting union of lanes into one dataframe before filtering")

    val unitedTuple = lanes.reduce((l1, l2) => {

      val j = l1._1.union(l2._1)

      l1._1.unpersist(true)
      l2._1.unpersist(true)

      j.show(false)

      (j, l1._2 + l2._2)
    })

    println("Finished union of lanes into one dataframe before filtering")

    val datasetSize = unitedTuple._2
    val unitedLanesDF = unitedTuple._1

//    val barcodesExtracted = unitedTuple._2

//    sanity

//    val datasetSize = unitedLanesDF.count()
//
//    println(s"accumulated lanes sizes is: ${unitedTuple._2}")
//    println(s"All lanes dataset size is:  $datasetSize")

    unitedLanesDF.show(false)

    val filteredDuplicatesDf = unitedLanesDF
      .dropDuplicates(KEY_BARCODE, KEY_READ, KEY_SEQUENCE)
      .withColumn(KEY_MIN_READ, substring(col(KEY_READ), 0, fastqConf.minBases))
      .withColumn(KEY_ACC_QUALITY_SCORE, accumulatedReadValueScoreUdf(col(KEY_QUALITY_SCORE)))

    unitedLanesDF.unpersist(true)

    val filteredDuplicates = filteredDuplicatesDf.count()

    println(s"Showing filtered duplicates after unique together KEY_BARCODE, KEY_READ, KEY_SEQUENCE duplicates were filtered")

    filteredDuplicatesDf
      .show(false)


    val rdd: RDD[Row] = filteredDuplicatesDf
      .rdd
      .groupBy(row => row.getAs[String](KEY_MIN_READ))
      .map(iterableTuple => {
        iterableTuple._2.reduce(byHigherTranscriptionQuality)
      })

    val filteredSimilarReadsDf = sqlContext.createDataFrame(rdd, filteredDuplicatesDf.schema)

    val filteredSimilarReads = filteredSimilarReadsDf.count()

    filteredSimilarReadsDf.show(false)

    println(s"\nShowing filtered dataset")

    filteredSimilarReadsDf
      .show(50, false)

    println(s"Dataset size:                  $datasetSize")
    println(s"After filtering duplicates:    $filteredDuplicates")
    println(s"After filtering similar:       $filteredSimilarReads\n")

    println(s"Filtered Duplicates:           ${datasetSize - filteredDuplicates}")
    println(s"Filtered Similar:              ${filteredDuplicates - filteredSimilarReads}\n")
    println(s"Total Filtered:                ${datasetSize - filteredSimilarReads}\n")

    println(s"Filtered dataset:              $filteredSimilarReads\n")

    toFastq(filteredSimilarReadsDf, sinkDir, sampleName, fs, sc)
  }

}
