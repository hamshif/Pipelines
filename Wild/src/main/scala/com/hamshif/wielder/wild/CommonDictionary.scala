package com.hamshif.wielder.wild

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * @author Gideon Bar
  *
  */
trait CommonDictionary {

  val DATE_FORMAT = "yyyy-MM-dd-HH-mm"

  val dateFormatter :DateTimeFormatter = DateTimeFormat.forPattern(DATE_FORMAT)

  val LOCKED_SUFFIX = ".locked"

  val OFFSET = "offset"

  val RAW = "raw"

  val REFINED = "refined"

  val KAFKA = "kafka"

  val MYSQL = "mysql"

  val FULL = "full"

  val INCREMENTAL = "incremental"

  val ACTIVITY = "activity"

  val DATA = "data"

  val REFINE_KAFKA = "refine_kafka"

  val ACTIVITIES_BY_DATE_BY_TIME = "activities_by_date_by_type"

  val EPOCH = "EPOCH"

  val KEY_MESSAGE = "message"
}
