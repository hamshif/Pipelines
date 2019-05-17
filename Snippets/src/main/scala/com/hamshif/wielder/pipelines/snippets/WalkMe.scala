package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

trait Keys {
  val KEY_TIMESTAMP = "timestamp"
  val KEY_ACTION = "action"
  val KEY_EMAIL = "email"
  val KEY_IP = "ip"
}

object WalkMe extends Keys {

  def main (args: Array[String]): Unit = {

    val rootDir = args(0)

    val viewsDir = s"${rootDir}views"
    val clicksDir = s"${rootDir}clicks"

    val sinkDir = s"$rootDir/backup"

    val standardColumnNames: Array[String] = Array(KEY_TIMESTAMP, KEY_ACTION, KEY_EMAIL, KEY_IP)


    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    import sparkSession.sqlContext.implicits._


    val salt = "melach_yam"

    val viewsDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .format("csv")
      .load(viewsDir)
      .withColumn(KEY_ACTION, lit("view"))
      .select(standardColumnNames.head, standardColumnNames.tail: _*)

    println("showing viewsDf")
    viewsDf.printSchema
    viewsDf.show(false)
    println("\n\n")

    val clicksDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .format("csv")
      .load(clicksDir)
      .withColumnRenamed("ts", KEY_TIMESTAMP)
      .withColumn(KEY_ACTION, concat(lit("click-"), $"element"))
      .drop("element")

    val enrichedClicks = clicksDf.as("clicksDf")
      .join(viewsDf.as("viewsDf"), clicksDf(KEY_EMAIL) === viewsDf(KEY_EMAIL))
      .select("clicksDf.timestamp", "clicksDf.action", "clicksDf.email", "viewsDf.ip")

    println("showing clicksDf")
    clicksDf.printSchema
    clicksDf.show(false)
    println("\n\n")


    println("showing enrichedClicks")
    enrichedClicks.printSchema
    enrichedClicks.show(false)
    println("\n\n")


    val unitedDF = viewsDf.union(enrichedClicks)
      .orderBy(asc(KEY_TIMESTAMP))


    println("showing unitedDF")
    unitedDF.printSchema
    unitedDF.show(false)
    println("\n\n")

    val maskedDf = maskDf(unitedDF, sparkSession, salt)

    println("showing maskedDf")
    maskedDf.printSchema
    maskedDf.show(false)
    println("\n\n")

    writeCSV(maskedDf
      .select("*")
      .where($"timestamp" < 1533081800), s"$sinkDir/first")

    writeCSV(maskedDf
      .select("*")
      .where($"timestamp" >= 1533081800), s"$sinkDir/second")
  }


  val maskedIpUdf = udf(maskIp)

  def maskIp = (s: String) => {

//    println(s"s is: $s")

    val masked = s
      .split('.')
      .dropRight(1)

    val d: String = masked.reduce((s1, s2) => s1 + "." +s2)

    d + ".0"
  }


  def joinReads(read1Df: DataFrame, read2Df: DataFrame): DataFrame  = {

    read1Df
      .join(read2Df, Seq(KEY_EMAIL),"outer")
  }


  def maskDf(inDf: DataFrame, sparkSession: SparkSession, salt: String) = {

    import sparkSession.sqlContext.implicits._

    val maskedDf = inDf

      .withColumn("salted", concat($"email", lit(salt)))
      .withColumn("hashed_email", md5($"salted"))
      .drop(KEY_EMAIL, "salted")
      .withColumn("masked_ip", maskedIpUdf($"ip"))
      .drop(KEY_IP)

    maskedDf
  }


  def writeCSV(inDf: DataFrame, destination: String) = {

    inDf
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save(destination)
  }

}
