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

        //    read1Df.printSchema()
        //
        //    read2Df.printSchema()
      }

      val joinedWithBarcode = joinFastqReadsWithFilteringExtractions(read1Df, read2Df, fastqConf.minBases)

      val barcodeSeparatedCardinality = joinedWithBarcode.count()

      println(s"Joined data from\n$t1\n$t2\n")

      if(fastqConf.debugVerbose){

        println(s"$t1 size is:                  $rawBarcodeDataCardinality")
        println(s"$t2 size is:                  $rawSequenceTailDataCardinality")
        println(s"barcodeSeparatedCardinality:  $barcodeSeparatedCardinality")

        joinedWithBarcode
          .show(false)
      }
      
      joinedWithBarcode :: acc
    })

    println("Finished creating read1 read2 pairs\n")

    println("Starting union of lanes into one dataframe before filtering\n")

    val unitedLanesDF = lanes.reduce((l1, l2) => {

      val j = l1.union(l2)

      if(fastqConf.debugVerbose){
        j.show(false)
      }
      
      j
    })

    println("Finished union of lanes into one dataframe before filtering")

    val datasetSize = unitedLanesDF.count()

    println(s"accumulated lanes size is: ${datasetSize}")

    if(fastqConf.debugVerbose){
      unitedLanesDF.show(false)
    }

    val filteredDuplicatesDf = unitedLanesDF
      .dropDuplicates(KEY_BARCODE, KEY_READ, KEY_SEQUENCE)

    val filteredDuplicates = filteredDuplicatesDf.count()

    println(s"Filtered Duplicates:           ${datasetSize - filteredDuplicates}")

    if(fastqConf.debugVerbose){

      println(s"Showing filtered duplicates")

      filteredDuplicatesDf.show(false)
    }


    val rdd1 = filteredDuplicatesDf
      .rdd
      .groupBy(row => row.getAs[String](KEY_MIN_READ))




//    val c = rdd1.count()
//
//    println(s"number of groups $c")
//
//    rdd1.foreach(it => {
//      println(s"\nkey: ${it._1}")
//
//      it._2.map(row => {
//
//        val j = row.getAs[String](KEY_MIN_READ)
//        println(s" value: ${j}")
//      })
//    })

    val rdd: RDD[Row] = rdd1
      .map(iterableTuple => {
        iterableTuple._2.reduce(byHigherTranscriptionQuality)
      })


    val filteredSimilarReadsDf = sqlContext.createDataFrame(rdd, filteredDuplicatesDf.schema)

    val filteredSimilarReads = filteredSimilarReadsDf.count()

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

    val fl = filteredSimilarReadsDf

    toFastq(filteredSimilarReadsDf, sinkDir, sampleName, fs, sc)
  }

}
