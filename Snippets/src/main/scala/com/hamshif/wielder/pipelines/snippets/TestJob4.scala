package com.hamshif.wielder.pipelines.fastq

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType}

import scala.collection.mutable


object TestJob4 {

  def main (args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.sqlContext.implicits._

    val rawDf = Seq(
      (1, "Moe",  "Slap",  7.9, 118),
      (2, "Larry",  "Spank",  8.0, 115),
      (3, "Curly",  "Twist", 6.0, 113),
      (4, "Laurel", "Whimper", 7.53, 119),
      (5, "Hardy", "Laugh", 6.0, 18),
      (6, "Charley",  "Ignore",   9.7, 115),
      (2, "Moe",  "Spank",  6.8, 118),
      (3, "Larry",  "Twist", 6.0, 115),
      (3, "Charley",  "fall", 9.0, 115)
    ).toDF("id", "name", "requisite", "funniness_of_requisite", "age")

    rawDf.show(false)
    rawDf.printSchema

    val rawSchema = rawDf.schema

    val fUdf = udf(reduceByFunniness, rawSchema)

    val nameUdf = udf(extractAge, IntegerType)

    val aggDf = rawDf
      .groupBy("name")
      .agg(
        count(struct("*")).as("count"),
        max(col("funniness_of_requisite")),
        collect_list(struct("*")).as("horizontal")
      )
      .withColumn("short", fUdf($"horizontal"))
      .withColumn("age", nameUdf($"short"))
      .drop("horizontal")

    aggDf.printSchema

    aggDf.show(false)
  }

  def reduceByFunniness= (x: Any) => {

    val d = x.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]

    val red = d.reduce((r1, r2) => {

      val funniness1 = r1.getAs[Double]("funniness_of_requisite")
      val funniness2 = r2.getAs[Double]("funniness_of_requisite")

      val r3 = funniness1 match {
        case a if a >= funniness2 =>
          r1
        case _ =>
          r2
      }

      r3
    })

    red
  }

  def extractAge = (x: Any) => {

    val d = x.asInstanceOf[GenericRowWithSchema]

    d.getAs[Int]("age")
  }
}
