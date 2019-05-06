package com.hamshif.wielder.pipelines.fastq

import com.hamshif.wielder.wild.{DatalakeConfig, FsUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @author Gideon Bar
  * An ETL for ingesting and filtering FASTQ format genetic sequencing text files.
  * As of now it pairs fastq files to Read 1 and Read 2, "Joins" them into 1 dataframe.
  * It then sequentially combines all paired dataframes into 1 dataframe.
  * The ETL then filters for duplicates and similar read2 entries, outputting a cardinal account of the filtering stages.
  * Each filtering stage is done sequentially to preserve the filtering account disregarding potential optimizations of combining filters
  */
object FastQExperiment extends FastQUtil with FastqArgParser with FsUtil with FastQKeys with Logging {

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)
    val fastqConf = getSpecificConf(args)

    FastQExperiment.start(conf, fastqConf)
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

//    TODO consider moving this into a folding filter after the group by
    val filteredDuplicatesDf = filteredFaultyRead1Df
      .dropDuplicates(KEY_UMI, KEY_BARCODE, KEY_SEQUENCE)
      .withColumn("id", monotonically_increasing_id)

    val filteredDuplicates = filteredDuplicatesDf.count()

    println(s"Finished filtering exact duplicate filtering and counting them\n")

    println(s"Without Duplicates dataset size is:           ${filteredDuplicates}")

    if(fastqConf.debugVerbose){

      println(s"Showing filtered duplicates")

      filteredDuplicatesDf.show(false)
    }

    val partitionWindow = Window
      .partitionBy(col(KEY_MIN_READ_BARCODE))

    val filteredSimilarReadsDf = filteredDuplicatesDf
        .withColumn("maxRead", max(KEY_ACC_QUALITY_SCORE).over(partitionWindow))
        .withColumn("count", count(KEY_ACC_QUALITY_SCORE).over(partitionWindow))
        .withColumn("minId", min("id").over(partitionWindow))
        .where(col("maxRead") === col(KEY_ACC_QUALITY_SCORE) && col("minId") === col("id") )
        .drop("maxRead")
        .drop("minId")
        .drop("id")

    filteredSimilarReadsDf
      .printSchema

    filteredSimilarReadsDf.show(false)


    filteredDuplicatesDf.unpersist(true)

    if(fastqConf.debugVerbose){

      val c = filteredSimilarReadsDf.count()

      println(s"number of groups $c")


    }

    filteredSimilarReadsDf.take(50).foreach(r => {

      val key = r.getAs[String](KEY_MIN_READ_BARCODE)
      val quality = r.getAs[Long](KEY_ACC_QUALITY_SCORE)
      val cardinality = r.getAs[Long]("count")

      println(s"Combined First ${fastqConf.minBases} read1 bases with Barcode $key had $cardinality best quality $quality")

    })

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

//    filteredSimilarReadsDf.persist()

    toFastq(filteredSimilarReadsDf, sinkDir, sampleName, fs, sc)
  }

}
