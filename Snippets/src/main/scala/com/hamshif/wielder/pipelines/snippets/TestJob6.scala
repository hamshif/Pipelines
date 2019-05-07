package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object TestJob6 {

  def main (args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    import sparkSession.sqlContext.implicits._

    val rawDf = Seq(
      ("Moe",  "Slap",  7.9, 118),
      ("Larry",  "Spank",  8.0, 115),
      ("Curly",  "Twist", 6.0, 113),
      ("Laurel", "Whimper", 7.53, 113),
      ("Hardy", "Laugh", 6.0, 118),
      ("Charley",  "Ignore",   9.7, 115),
      ("Moe",  "Spank",  6.8, 118),
      ("Larry",  "Twist", 6.0, 115),
      ("Charley",  "fall", 9.0, 115)
    ).toDF("name", "requisite", "funniness_of_requisite", "age")

    rawDf.show(false)
    rawDf.printSchema

    val nameWindow = Window
      .partitionBy("name")

    val aggDf = rawDf
      .withColumn("id", monotonically_increasing_id)
      .withColumn("maxFun", max("funniness_of_requisite").over(nameWindow))
      .withColumn("count", count("name").over(nameWindow))
      .withColumn("minId", min("id").over(nameWindow))
      .where(col("maxFun") === col("funniness_of_requisite") && col("minId") === col("id") )
      .drop("maxFun")
      .drop("minId")
      .drop("id")

    aggDf.printSchema

    aggDf.show(false)
  }

}
