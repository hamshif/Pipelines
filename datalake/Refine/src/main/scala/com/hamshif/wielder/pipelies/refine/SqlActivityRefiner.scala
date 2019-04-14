package com.hamshif.wielder.pipelies.refine

import com.hamshif.wielder.wild._
import com.hamshif.wielder.wild.DatalakeConfig
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time._


object SqlActivityRefiner extends DatalakeArgParser with FsUtil with Logging{

  def main (args: Array[String]): Unit = {

    val conf = getConf(args)

    SqlActivityRefiner.start(conf)
  }

  def start (conf: DatalakeConfig): Unit = {

    val tableName = conf.mySqlTable

    val sparkSession = SparkSession
      .builder()
      .appName(SqlActivityRefiner.getClass.getName)
      .master(conf.sparkMaster)
      .getOrCreate()

    val munchkinBucketName = f"${conf.bucketName}"

    val baseAccountPath = s"${conf.fsPrefix}${munchkinBucketName}"

    val sourcePath = new Path(baseAccountPath)
    val fs = sourcePath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)

    logInfo(s"file exists: ${fs.exists(sourcePath)}")
    logInfo(s"file is directory: ${fs.isDirectory(sourcePath)}")

    val subTargets = conf.subDirs.split(",")

    val dirs = subTargets.foldLeft(List[FileStatus]())((acc, subTarget) => {
      val path = new Path(s"$baseAccountPath/$RAW/$MYSQL/$subTarget/$tableName")

      fs.exists(path) match {
        case true =>
          val ss = subDirs(fs, fs.getFileStatus(path), fsDirTimestampFilter)
          acc ::: ss
        case _ =>
          acc
      }
    })

    val syncDirPath = s"$baseAccountPath/$REFINED/activities_by_date_by_type"

    logInfo("\nall sub dirs that have a name parsable as date")
    //    The verbocity is for debugging
    val sourcePaths = dirs
      .map(s => {

        val dir_name = s.getPath.toString.split("/").last
        val date: DateTime = DateTime.parse(dir_name, dateFormatter)
        val asTime = date.getMillis

        logInfo(s"$asTime - $dir_name")

        (asTime, dir_name, s, s.getPath.toString)
      })
//      .filter(_._1 > offset._1)

//    debug(s"\nall sub dirs above date ${offset._1}")

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

//    val lockedOffsets = lockOffsets(fs, offset._2, s"$offsetDirPath/${latestSource._1}")

    logInfo(s"ingestionType: ${conf.ingestionType}")

    val df = sparkSession
      .read
      .format(conf.ingestionType)
      .load(inputDirs: _*)
//      .withColumnRenamed(DB_KEY_ACTIVITY_TYPE_ID, KEY_TYPE)
//      .withColumn(s"_${KEY_TYPE}", col(KEY_TYPE))

    df.show
    df.printSchema


    logDebug("")

    //Transform

//    df.select(KEY_OCCURRED_AT)
//      .map(line => )


    val activitiesDf = df

////      TODO find out if updated mysql corresponds to kafka persisted at
//      .withColumnRenamed(DB_KEY_UPDATED_AT, KEY_OCCURRED_AT)
//
////      TODO make sure the date is translated properly
////      .withColumn(KEY_DATE, from_unixtime(col(KEY_OCCURRED_AT).divide(1000)))
////      .withColumn(KEY_DATE, to_date(col(KEY_OCCURRED_AT).divide(1000).cast("timestamp")))
//      .withColumn(KEY_DATE, to_date(from_unixtime(col(KEY_OCCURRED_AT).divide(ADJUST_LONG_SIZE), "yyyy-MM-dd")))
//
//      //      .withColumn(KEY_DATE, longToEpochUDF(col(KEY_OCCURRED_AT))
//      .withColumn(KEY_YEAR, year(col(KEY_DATE)))
//      .withColumn(KEY_MONTH, month(col(KEY_DATE)))
////      .withColumn(KEY_DAY, dayofmonth(col(KEY_DATE)))
//      .drop(KEY_DATE)
//
//      .withColumnRenamed(DB_KEY_ACTION_RESULT_CODE, KEY_ACTION_RESULT_CODE)
//      .withColumnRenamed(DB_KEY_ACTIVITY_TYPE_ID, KEY_ACTIVITY_TYPE_ID)
//      .withColumnRenamed(DB_KEY_ATTRIBUTE_VALUES, KEY_ATTRIBUTES)
//      .withColumnRenamed(DB_KEY_BYPASS_TRIGGER, KEY_BYPASS_TRIGGER)
//      .withColumnRenamed(DB_KEY_CAMPAIGN_ID, KEY_CAMPAIGN_ID)
//      .withColumnRenamed(DB_KEY_CREATED_AT, KEY_CREATED_AT)
//      .withColumnRenamed(DB_KEY_LEAD_PERSON_ID, KEY_LEAD_PERSON_ID)
//      .withColumnRenamed(DB_KEY_MKTG_ASSET_ID, KEY_PRIMARY_ASSET_ID)
//      .withColumnRenamed(DB_KEY_MKTG_ASSET_NAME, KEY_PRIMARY_ASSET_NAME)
//
////      TODO make sure that sys_action_date is a derivative of created_at and updated_at
//      .withColumnRenamed(DB_KEY_SYS_ACTION_DATE, KEY_PERSISTED_AT)
//
////      TODO make sure that created at is a db artifact from another activity namely the first activity
//      .drop(DB_KEY_CREATED_AT)
//
////      Removing autoincrement from MySQL table
//      .drop(KEY_ID)



////      ADD missing fields to get the same schema as kafka
//    .withColumn(KEY_SEQUENCE_NUM, typedLit(0L))
//    .withColumn(KEY_IS_SYNTHETIC, typedLit(true))
//    .withColumn(KEY_SOURCE, lit(null: String).cast(StringType))
//    .withColumn(KEY_IS_STORED, typedLit(true))
//    .withColumn(KEY_IS_PENDING, typedLit(false))
//    .withColumn(KEY_STORE_STATUS, typedLit(false))
//    .withColumn(KEY_MUNCHKIN_ID, typedLit("{}"))
//    .withColumn(KEY_TARGET, lit(null: String).cast(StringType))
//    .withColumn(KEY_IS_RECOVERED, typedLit(false))
//    .withColumn(KEY_ID, lit(null: String).cast(StringType))
//    .withColumn(KEY_LEAD_PARTITION, lit(null: String).cast(StringType))
//    .withColumn(KEY_PERSON, lit(null: String).cast(StringType))
//    .withColumn(KEY_IS_ANONYMOUS, typedLit(false))
//    .withColumn(KEY_ACTIVITY_TYPE_ID, typedLit(0L))



    activitiesDf.printSchema

//    activitiesDf.select(KEY_MUNCHKIN_ID).show(false)

    activitiesDf.show()


//    activitiesDf.select(KEY_ID, KEY_OCCURRED_AT, KEY_DATE).show(false)
//    activitiesDf.select(KEY_ATTRIBUTES).show(false)


    logDebug("")

    //Sink
    activitiesDf
      .write
      .mode(SaveMode.Append)
      .format(conf.syncType)
//      .partitionBy(KEY_YEAR, KEY_MONTH, s"_${KEY_TYPE}")
      .save(syncDirPath)

    //Remove from cache
    //    df.unpersist()
    //    activitiesDf.unpersist()
  }
}
