package com.hamshif.wielder.pipelines.fastq

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.collection.mutable


object TestJob {

  def main (args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName.replace("$", ""))
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.sqlContext.implicits._


    val df1 = Seq(
      ("key1", ("internalKey1", "value1")),
      ("key1", ("internalKey2", "value2")),
      ("key2", ("internalKey3", "value3")),
      ("key2", ("internalKey4", "value4")),
      ("key2", ("internalKey5", "value5"))
    )
      .toDF("name", "merged")

//    df1.printSchema
//
//    df1.show(false)

    val res = df1
      .groupBy("name")
      .agg( collect_list(col("merged")).as("final") )

    res.printSchema

    res.show(false)

    def f= (x: Any) => {

      val d = x.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]

      val d1 = d.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]].head

      d1.toString
    }

    val fUdf = udf(f, StringType)

    val d2 = res
      .withColumn("d", fUdf(col("final")))
      .drop("final")

    d2.printSchema()

    d2
      .show(false)
  }
}
