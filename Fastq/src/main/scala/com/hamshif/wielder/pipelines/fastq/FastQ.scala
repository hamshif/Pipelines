package com.hamshif.wielder.pipelines.fastq

import com.hamshif.wielder.wild.{DatalakeArgParser, DatalakeConfig, FsUtil}
import org.apache.hadoop.fs.{Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col}

/**
  * @author Gideon Bar
  * An ETL for ingesting and filtering FASTQ format genetic sequencing text files.
  * As of now it pairs fastq files to Read 1 and Read 2, "Joins" them into 1 dataframe.
  * It then sequentially combines all paired dataframes into 1
  * and filters for duplicates outputting a cardinality account of the filtering stages.
  */
object FastQ extends FastQUtil with DatalakeArgParser with FsUtil with FastQKeys with Logging {

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)

    FastQ.start(conf)
  }


  def start(conf: DatalakeConfig): Unit = {

    //    val basePath = s"gs://gid-ram/ram"

//    val basePath = "/Users/gbar/ram/short"

    val fastqDir = f"${conf.bucketName}"

    val basePath = s"${conf.fsPrefix}${fastqDir}"

    val sinkDir = s"$basePath-filtered"

    val sparkSession = SparkSession
      .builder()
      .appName(FastQ.getClass.getName)
      .master(conf.sparkMaster)
      .getOrCreate()

    val sc = sparkSession.sparkContext
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

      j.show(false)

      (j, l1._2 + l2._2)
    })

    println("Finished union of lanes into one dataframe before filtering")

    val united = unitedTuple._1
    val unitedLanesCardinality = united.count()

    println(s"All lanes dataset size is: $unitedLanesCardinality")

    united.show(false)

    val filteredDuplicates = united.dropDuplicates(KEY_BARCODE, KEY_READ, KEY_SEQUENCE)

    val filteredDuplicatesCardinality = filteredDuplicates
      .count()

    val barcodeSeparatedCardinality = unitedTuple._2

    val duplicates = barcodeSeparatedCardinality - filteredDuplicatesCardinality


    println(s"Showing filteredDuplicates after unique together KEY_BARCODE, KEY_READ, KEY_SEQUENCE duplicates were filtered")
    println(s"$duplicates duplicates were filtered")
//    println(s"Showing only these columns: $KEY_UNIQUE, $KEY_S_SEQUENCE, $KEY_SEQUENCE")

    filteredDuplicates
//      .select(col(KEY_UNIQUE), col(KEY_S_SEQUENCE), col(KEY_SEQUENCE))
      .show(false)


    val filteredSuspicious = filteredDuplicates.dropDuplicates(KEY_BARCODE, KEY_READ)

    val filteredSuspiciousCardinality = filteredSuspicious.count()

    val suspiciousDuplicatesCardinality = filteredDuplicatesCardinality - filteredSuspiciousCardinality

    println(s"Showing filteredSuspicious after unique together KEY_BARCODE, KEY_READ duplicates were filtered")
    println(s"$suspiciousDuplicatesCardinality suspicious entries were filtered")
//    println(s"Showing only these columns: $KEY_UNIQUE, $KEY_S_SEQUENCE, $KEY_SEQUENCE")
    filteredSuspicious
//      .select(col(KEY_UNIQUE), col(KEY_BARCODE), col(KEY_READ))
      .show(false)

    println(s"unitedLanesCardinality:           $unitedLanesCardinality")

    println(s"filteredDuplicates:               $filteredDuplicatesCardinality")

    println(s"suspiciousDuplicatesCardinality:  $suspiciousDuplicatesCardinality")

    println(s"After all filtering:              $filteredSuspiciousCardinality")


    toFastq(filteredSuspicious, sinkDir, sampleName, fs, sc)
  }

}
