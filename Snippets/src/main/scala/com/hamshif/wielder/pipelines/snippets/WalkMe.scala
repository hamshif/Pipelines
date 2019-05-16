package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object WalkMe {

  def main (args: Array[String]): Unit = {

    val rootDir = args(0)

    val viewsDir = s"${rootDir}views"
    val clicksDir = s"${rootDir}clicks"

    val sincDir = s"$rootDir/backup"

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
//
    import sparkSession.sqlContext.implicits._

    val salt = "melach_yam"


    val clicksDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .format("csv")
      .load(clicksDir)
      .withColumnRenamed("ts", "timestamp")
      .withColumn("action", concat(lit("click-"), $"element"))

    clicksDf.printSchema

    println("showing clicksDf")
    clicksDf.show(false)

    val viewsDf = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .format("csv")
      .load(viewsDir)
      .withColumn("action", lit("view"))


    viewsDf.printSchema
    println("showing viewsDf")
    viewsDf.show(false)

    val joinedClicks = joinReads(clicksDf, viewsDf.drop("timestamp"))

    println("showing joinedClicks")
    joinedClicks
      .show(false)


     val joinedViews = joinReads(viewsDf, clicksDf.drop("timestamp"))


    println("showing joinedViews")
    joinedViews.show(false)


    val hashedViewsDf = hashDf(joinedViews, sparkSession, salt)

    println("showing hashedViewsDf")
    hashedViewsDf.show(false)


    val hashedViewsDfTodo = hashedViewsDf
      .drop("action")

//    TODO solve duplicate column problem
    toCSV(hashedViewsDfTodo, destination = s"$sincDir/views")


    val hashedClicksDf = hashDf(joinedViews, sparkSession, salt)
      .filter($"element".isNotNull)

    println("showing hashedClicksDf")
    hashedClicksDf.show(false)


    val hashedClicksDfTodo = hashedClicksDf
      .drop("action")

//    TODO solve duplicate column problem
    toCSV(hashedClicksDfTodo, destination = s"$sincDir/clicks")

  println("break")

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
      .join(read2Df, Seq("email"),"outer")
  }


  def hashDf(inDf: DataFrame, sparkSession: SparkSession, salt: String) = {

    import sparkSession.sqlContext.implicits._

    val maskedDf = inDf
      .orderBy(asc("timestamp"))
      .withColumn("salted", concat($"email", lit(salt)))
      .withColumn("hashed_email", md5($"salted"))
      .drop("email", "salted")
      .withColumn("masked_ip", maskedIpUdf($"ip"))
      .drop("ip")

    maskedDf
  }


  def toCSV(inDf: DataFrame, destination: String) = {

    inDf
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"$destination.csv")
  }

}
