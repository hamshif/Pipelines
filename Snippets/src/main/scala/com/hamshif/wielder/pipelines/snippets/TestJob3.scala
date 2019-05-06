package com.hamshif.wielder.pipelines.fastq

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable


object TestJob3 {

  def main (args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.sqlContext.implicits._

    val rawDf = Seq(
      (1, "Moe",  "Slap",  2.0, 18),
      (2, "Larry",  "Spank",  3.0, 15),
      (3, "Curly",  "Twist", 5.0, 15),
      (4, "Laurel", "Whimper", 3.0, 9),
      (5, "Hardy", "Laugh", 6.0, 18),
      (6, "Charley",  "Ignore",   5.0, 5)
    ).toDF("id", "name", "requisite", "money", "age")

    rawDf.show(false)
    rawDf.printSchema

    val rawSchema = rawDf.schema

    val fUdf = udf(reduceByMoney, rawSchema)

    val nameUdf = udf(extractName, StringType)

    val aggDf = rawDf
      .groupBy("age")
      .agg(
        count(struct("*")).as("count"),
        max(col("money")),
        collect_list(struct("*")).as("horizontal")
      )
      .withColumn("short", fUdf($"horizontal"))
      .withColumn("name", nameUdf($"short"))
      .drop("horizontal")

    aggDf.printSchema

    aggDf.show(false)

  }

  def reduceByMoney= (x: Any) => {

    val d = x.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]

    val red = d.reduce((r1, r2) => {

      val money1 = r1.getAs[Double]("money")
      val money2 = r2.getAs[Double]("money")

      val r3 = money1 match {
        case a if a >= money2 =>
          r1
        case _ =>
          r2
      }

      r3
    })

    red
  }

  def extractName = (x: Any) => {

    val d = x.asInstanceOf[GenericRowWithSchema]

    d.getAs[String]("name")
  }
}
