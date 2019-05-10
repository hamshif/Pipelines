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
  * It then sequentially combines all paired dataframes into 1 dataframe.
  * The ETL then filters for duplicates and similar read2 entries, outputting a cardinal account of the filtering stages.
  * Each filtering stage is done sequentially to preserve the filtering account disregarding potential optimizations of combining filters
  */
object FastQE2 extends FastQUtil with FastqArgParser with FsUtil with FastQKeys with Logging {

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)
    val fastqConf = getSpecificConf(args)

    FastQE2.start(conf, fastqConf)
  }


  def start(conf: DatalakeConfig, fastqConf: FastqConfig): Unit = {

    val fastqDir = f"${conf.bucketName}"

    val basePath = s"${conf.fsPrefix}${fastqDir}"

    val sinkDir = s"$basePath-filtered"

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master(conf.sparkMaster)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    sc.setLogLevel("ERROR")
    sc.getConf.set("spark.debug.maxToStringFields", "100")

    val sqlContext = sparkSession.sqlContext


    val fs = new Path(basePath).getFileSystem(sc.hadoopConfiguration)

    val byLane = getFastqFilesByLane(sparkSession, basePath, fs)

    val sampleName = byLane.head._2._1
      .split("/").last
      .split("_").head

    println(s"Starting to read fastq files and join read1 read2 pairs\n")

    val lanes = byLane.foldLeft((List[DataFrame]()))((acc, laneTuple) => {

      val t1 = laneTuple._2._1.replace("file:", "")
      val t2 = laneTuple._2._2.replace("file:", "")

      val read1Df: DataFrame = getSequenceDf(sparkSession, sc, sqlContext, t1, true)

      val read2Df: DataFrame = getSequenceDf(sparkSession, sc, sqlContext, t2, false)

      var rawBarcodeDataCardinality = 0L
      var rawSequenceTailDataCardinality = 0L
      
      if(fastqConf.debugVerbose){
        
        read1Df.show(30, false)

        read2Df.show(30, false)

        rawBarcodeDataCardinality = read1Df.count()
        rawSequenceTailDataCardinality = read2Df.count()
      }

      val joinedWithBarcode = joinFastqReadsWithFilteringExtractions(read1Df, read2Df, fastqConf.minBases)

      println(s"Joined data from\n$t1\n$t2\n")

      read1Df.unpersist(true)
      read2Df.unpersist(true)

      if(fastqConf.debugVerbose){

        val barcodeSeparatedCardinality = joinedWithBarcode.count()

        println(s"$t1 size is:                  $rawBarcodeDataCardinality")
        println(s"$t2 size is:                  $rawSequenceTailDataCardinality")
        println(s"barcodeSeparatedCardinality:  $barcodeSeparatedCardinality")

        joinedWithBarcode
          .show(false)
      }
      
      joinedWithBarcode :: acc
    })

    println(s"Finished creating read1 read2 pairs and extracting derived columns\n")
    println("Starting union of lanes into one dataframe before filtering\n")

    val unitedLanesDF = lanes.reduce((l1, l2) => {

      val j = l1.union(l2)

      l2.unpersist(true)

      println("reducing into one iteration")

      if(fastqConf.debugVerbose){
        j.show(false)
      }

      j
    })

    println("Finished union of lanes into one dataframe before filtering")

    val datasetSize = unitedLanesDF.count()

    println(s"accumulated lanes size is: ${datasetSize}")

    if(fastqConf.debugVerbose){
      unitedLanesDF.show(50,false)
    }


    val filteredFaultyRead1Df = unitedLanesDF
      .filter(x => {
        val shortSequence = x.getAs[String](KEY_S_SEQUENCE)
        ! shortSequence.contains("N")
      })


    if(fastqConf.debugVerbose){
      filteredFaultyRead1Df.show(50, false)
    }

    val filteredFaultyRead1 = filteredFaultyRead1Df.count()
    println(s"After Filtering read1 with 'N' misreads size is: ${filteredFaultyRead1}")

//    TODO UMI and Barcode are derived from short sequence but this is more verbose
    val filteredDuplicatesDf = filteredFaultyRead1Df
      .dropDuplicates(KEY_UMI, KEY_BARCODE, KEY_SEQUENCE)

    val filteredDuplicates = filteredDuplicatesDf.count()

    println(s"Finished filtering exact duplicate filtering and counting them\n")

    println(s"Without Duplicates dataset size is:           ${filteredDuplicates}")

    if(fastqConf.debugVerbose){

      println(s"Showing filtered duplicates")

      filteredDuplicatesDf.show(false)
    }

    filteredDuplicatesDf.printSchema

    filteredDuplicatesDf.show(false)

    val maxQualityUdaf = new KeepRowWithMaxQuality

    val statsDf = filteredDuplicatesDf
      .groupBy(KEY_MIN_READ, KEY_BARCODE)
      .agg(
        count(col(KEY_MIN_READ_BARCODE)),
        max(col(KEY_ACC_QUALITY_SCORE)),
        maxQualityUdaf(
          col(KEY_SEQUENCE_IDENTIFIER),
          col(KEY_SEQUENCE),
          col(KEY_QUALITY_SCORE_IDENTIFIER),
          col(KEY_QUALITY_SCORE),

          col(KEY_S_SEQUENCE_IDENTIFIER),
          col(KEY_S_SEQUENCE),
          col(KEY_S_QUALITY_SCORE_IDENTIFIER),
          col(KEY_S_QUALITY_SCORE),

          col(KEY_ACC_QUALITY_SCORE)
        ).as(KEY_ROW_WITH_MAX_QUALITY)
      )

    statsDf.printSchema

    statsDf.show(false)

    val filteredDuplicatesSchema = filteredDuplicatesDf.schema

    val rdd: RDD[Row] = filteredDuplicatesDf
      .rdd
      .groupBy(row => {
        row.getAs[String](KEY_MIN_READ_BARCODE)
      })
      .map(iterableTuple => {
        iterableTuple._2
          .reduce(byHigherTranscriptionQuality1)
      })

    filteredDuplicatesDf.unpersist(true)

    val filteredSimilarReadsDf = sqlContext.createDataFrame(rdd, filteredDuplicatesSchema)

    rdd.unpersist(true)

    val filteredSimilarReads = filteredSimilarReadsDf.count()

    println(s"Finished filtering similar reads and counting them.\n")







    if(fastqConf.debugVerbose) {

      println(s"\nShowing filtered dataset")
      filteredSimilarReadsDf
        .show(50, false)
    }

    println(s"Dataset size:                  $datasetSize")
    println(s"After filtering read1 with N:  $filteredFaultyRead1")
    println(s"After filtering duplicates:    $filteredDuplicates")
    println(s"After filtering similar:       $filteredSimilarReads\n")

    println(s"Filtered Faulty read1:         ${datasetSize - filteredFaultyRead1}")
    println(s"Filtered Duplicates:           ${filteredFaultyRead1 - filteredDuplicates}")
    println(s"Filtered Similar:              ${filteredDuplicates - filteredSimilarReads}\n")
    println(s"Total Filtered:                ${datasetSize - filteredSimilarReads}\n")

    println(s"Filtered dataset:              $filteredSimilarReads\n")


    toFastq(filteredSimilarReadsDf, sinkDir, sampleName, fs, sc)
  }

}
