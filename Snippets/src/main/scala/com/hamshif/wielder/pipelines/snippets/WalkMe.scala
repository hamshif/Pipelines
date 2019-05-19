package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object WalkMe extends CommonKeys {

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
      .withColumn(KEY_TIMESTAMP, toLongUdf($"timestamp"))
      .select(standardColumnNames.head, standardColumnNames.tail: _*)

    showDf(viewsDf, "viewsDf")

    val clicksDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .format("csv")
      .load(clicksDir)
      .withColumnRenamed("ts", KEY_TIMESTAMP)
      .withColumn(KEY_TIMESTAMP, toLongUdf($"timestamp"))
      .withColumn(KEY_ACTION, concat(lit("click-"), $"element"))
      .drop("element")

    showDf(clicksDf, "clicksDf")

    val enrichedClicks = clicksDf.as("clicksDf")
      .join(viewsDf.as("viewsDf"), clicksDf(KEY_EMAIL) === viewsDf(KEY_EMAIL))
      .select("clicksDf.timestamp", "clicksDf.action", "clicksDf.email", "viewsDf.ip")

    showDf(enrichedClicks, "enrichedClicks")

    val unitedDF = viewsDf.union(enrichedClicks)
      .withColumn(KEY_INTERVAL, closest200IntervalUdf(col(KEY_TIMESTAMP)))
      .orderBy(asc(KEY_TIMESTAMP))

    showDf(unitedDF, "unitedDF")

    val maskedDf = maskDf(unitedDF, sparkSession, salt)

    showDf(maskedDf, "maskedDf")

    maskedDf
      .repartition(col(KEY_INTERVAL))
      .write.format("com.databricks.spark.csv")
      .partitionBy(KEY_INTERVAL)
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save(sinkDir)
  }

  val toLongUdf = udf(flam)

  def flam = (i: Int) => {

    i.toLong
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


  def showDf(df: DataFrame, name: String) = {

    println(s"showing $df")
    df.printSchema
    df.show(false)
    println("\n\n")
  }

  val closest200IntervalUdf = udf(closest200Interval)

  def closest200Interval = (n: Long) => {

    val a = n / 100

    if(a % 2 != 0)
      a - 1
    else a
  }

}
