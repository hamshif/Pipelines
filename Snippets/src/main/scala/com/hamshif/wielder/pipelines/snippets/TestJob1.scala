package com.hamshif.wielder.pipelines.fastq

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

import scala.collection.mutable


object TestJob1 {

  def main (args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.sqlContext.implicits._

    val rawDf = Seq(
      ("key", 1L, "gargamel"),
      ("key", 4L, "pe_gadol"),
      ("key", 2L, "zaam"),
      ("key1", 5L, "naval")
    ).toDF("group", "quality", "other")

    rawDf.show(false)
    rawDf.printSchema

    val rawSchema = rawDf.schema

    val fUdf = udf(reduceByQuality, rawSchema)

    val aggDf = rawDf
      .groupBy("group")
      .agg(
        count(struct("*")).as("num_reads"),
        max(col("quality")).as("quality"),
        collect_list(struct("*")).as("horizontal")
      )
      .withColumn("short", fUdf($"horizontal"))
      .drop("horizontal")


    aggDf.printSchema

    aggDf.show(false)
  }

  def reduceByQuality= (x: Any) => {

    val d = x.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]

    val red = d.reduce((r1, r2) => {

      val quality1 = r1.getAs[Long]("quality")
      val quality2 = r2.getAs[Long]("quality")

      val r3 = quality1 match {
        case a if a >= quality2 =>
          r1
        case _ =>
          r2
      }

      r3
    })

    red
  }
}
