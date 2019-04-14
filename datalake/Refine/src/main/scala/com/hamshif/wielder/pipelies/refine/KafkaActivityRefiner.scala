package com.hamshif.wielder.pipelies.refine

import com.hamshif.wielder.wild._
import com.hamshif.wielder.wild.DatalakeConfig
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime


object KafkaActivityRefiner extends DatalakeArgParser with FsUtil with Logging {

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)

    KafkaActivityRefiner.start(conf)
  }

  def start (conf: DatalakeConfig): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName(KafkaActivityRefiner.getClass.getName)
      .master(conf.sparkMaster)
      .getOrCreate()

    val munchkinBucketName = f"${conf.bucketName}"

    val baseAccountPath = s"${conf.fsPrefix}${munchkinBucketName}"

    val sourceDirPath = s"$baseAccountPath/$RAW/$KAFKA"

    logInfo(s"Trying to read from:\n${sourceDirPath}")

    val sourcePath = new Path(sourceDirPath)
    val fs = sourcePath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)

    logInfo(s"file exists: ${fs.exists(sourcePath)}")
    logInfo(s"file is directory: ${fs.isDirectory(sourcePath)}")

    val syncDirPath = s"$baseAccountPath/$REFINED/$ACTIVITIES_BY_DATE_BY_TIME"

    val offsetState = getOrCreateOffset(fs, baseAccountPath, REFINE_KAFKA)
    logInfo(s"Offset: $offsetState")

    if(offsetState.isLocked){

      logInfo("Offset is locked TODO add supervision in logs")

      return
    }

    val dirs: List[FileStatus] = subDirs(fs, fs.getFileStatus(sourcePath), fsDirTimestampFilter)

    logInfo("\nall sub dirs that have a name parsable as date")
    //    The verbosity is for debugging
    val sourcePaths = dirs
      .map(s => {

        val dir_name = s.getPath.toString.split("/").last
        val date: DateTime = DateTime.parse(dir_name, dateFormatter)
        val asTime = date.getMillis

        logInfo(s"$asTime - $dir_name")

        (asTime, dir_name, s, s.getPath.toString)
      })
      .filter(t => t._1 > offsetState.offset)

    logInfo(s"\nall sub dirs above date ${offsetState.offset}")

    if(sourcePaths.size == 0){
      logInfo(s"nothing to move: ")

      return
    }

    val inputDirs = sourcePaths.map(s => {
      logInfo(s"${s._1} - ${s._2}")

      s._4
    })


    val latestSource = sourcePaths.reduceLeft((x, y) => if(x._1 > y._1) x else y)

    logInfo(s"new offset is: ${latestSource._1}")

    val lockedOffsets = lockOffsets(fs, offsetState.fileStatus, s"${offsetState.basePath}/${latestSource._1}")

    logInfo(s"ingestionType: ${conf.ingestionType}")

    val df = sparkSession
      .read
      .format(conf.ingestionType)
      .load(inputDirs: _*)


    df.printSchema

    df.show

    logDebug("")

    //Transform
    //using only nested json activity, flattening first an second dictionaries
    val activitiesDf = df
//      //      TODO see why raw is partially refined
//      //      .withColumnRenamed("_1", KEY_KAFKA_TOPIC)
//      //      .withColumnRenamed("_2", KEY_JSON_ACTIVITY)
////        .withColumnRenamed(KEY_MESSAGE, KEY_JSON_ACTIVITY)
////        .select(from_json(col(KEY_JSON_ACTIVITY), activityJsonSchema) as ACTIVITY)
////        .select(col(s"$ACTIVITY.*"))
////        .withColumn(DATA, from_json(col(KEY_DATA), activityDataJsonSchema))
////        .select(col(s"*"), col(s"$DATA.*"))
////        .drop(DATA)
////        .withColumn(KEY_DATE, to_date(col(KEY_OCCURRED_AT)))
////        .withColumn(KEY_YEAR, year(col(KEY_DATE)))
////        .withColumn(KEY_MONTH, month(col(KEY_DATE)))
//////        .withColumn(KEY_DAY, dayofmonth(col(KEY_DATE)))
////        .drop(KEY_DATE)
//
//        // Transforming to long as in MySQL to avoid zone issues
//        .withColumn(EPOCH, col(KEY_OCCURRED_AT).cast(LongType).multiply(ADJUST_LONG_SIZE))
//        .drop(KEY_OCCURRED_AT)
//
//        .withColumnRenamed(EPOCH, KEY_OCCURRED_AT)
//        .withColumn(EPOCH, col(KEY_PERSISTED_AT).cast(LongType).multiply(ADJUST_LONG_SIZE))
//        .drop(KEY_PERSISTED_AT)
//        .withColumnRenamed(EPOCH, KEY_PERSISTED_AT)
//
//        .withColumn(EPOCH, col(KEY_CREATED_AT).cast(LongType).multiply(ADJUST_LONG_SIZE))
//        .drop(KEY_CREATED_AT)
//        .withColumnRenamed(EPOCH, KEY_CREATED_AT)
//
//        .withColumn("place_holder", col(KEY_TYPE).cast(LongType))
//        .drop(KEY_TYPE)
//        .withColumnRenamed("place_holder", KEY_TYPE)
//        .withColumn(s"_${KEY_TYPE}", col(KEY_TYPE))
//
    //    activitiesDf.select(KEY_ID).show(false)

//    activitiesDf.select(KEY_OCCURRED_AT, KEY_PERSISTED_AT).show

    activitiesDf.printSchema
    activitiesDf.show()

//    activitiesDf.select(KEY_STORE_STATUS).show(false)

    logDebug("")

    //Sync
    activitiesDf
      .write
      .mode(SaveMode.Append)
      .format(conf.syncType)
//      .partitionBy(KEY_YEAR, KEY_MONTH, s"_${KEY_TYPE}")
      .save(syncDirPath)

    unlockOffset(fs, lockedOffsets._1, lockedOffsets._2)
    //Remove from cache
//    df.unpersist()
//    activitiesDf.unpersist()
  }
}
