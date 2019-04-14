package com.hamshif.wielder.pipelines.mysql_ingestion

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * @author Gideon Bar
  * TODO this is still a very basic POC
  */
object SqlActivityIngestor extends SqlArgParser with SqlKeys {

  def main(args: Array[String]): Unit = {

    val conf = getConf(args)
    val sqlConf = getSpecificConf(args)

    val fsPrefix = conf.fsPrefix
    val bucket = conf.bucketName
    val appName = this.getClass.getSimpleName.replace("$", "")


    val sqlHost = s"$MYSQL_CONNECTION_PREFIX${sqlConf.host}:${sqlConf.port}/${sqlConf.db}"

    val tableList = sqlConf.tables.split(",")

    val directory = "pooh"
    val TimeZoneName = java.util.TimeZone.getDefault.getID

    //Build spark sql context
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(conf.sparkMaster)

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val dbUrl = s"$sqlHost?user=${sqlConf.user}&password=${sqlConf.password}&useLegacyDatetimeCode=false&serverTimezone=$TimeZoneName"
    //Get schema version
    val schemaVersion = 1
    //Build working directory
    val date = DateTime.now().toString()
    val workingDir = s"$fsPrefix/$bucket/$directory/$schemaVersion/$date/"
    //Get all tables
    //Get the info schema table
    //Save all tables
    tableList.map { tableName =>
      val prop = new java.util.Properties
      prop.setProperty("driver", MYSQL_DRIVER)
      //Read SQL table
      val df = sqlContext.read.jdbc(dbUrl, tableName, prop)

      df.printSchema()
      df.show()
      print("count is " + df.count())
      //Save table to avro file
      df.write.format("com.databricks.spark.avro").save(workingDir + tableName)
      //Remove from cache
      df.unpersist()
    }
  }
}
