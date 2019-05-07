    package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


    object ReduceAggJob {

      def main (args: Array[String]): Unit = {

        val appName = this.getClass.getName.replace("$", "")
        println(s"appName: $appName")

        val sparkSession = SparkSession
          .builder()
          .appName(appName)
          .master("local")
          .getOrCreate()

        val sc = sparkSession.sparkContext
        sc.setLogLevel("ERROR")

        import sparkSession.sqlContext.implicits._

        val rawDf = Seq(
          ("West",  "Apple",  2.0, 10),
          ("West",  "Apple",  3.0, 15),
          ("West",  "Orange", 5.0, 15),
          ("West",  "Orange", 17.0, 15),
          ("South", "Orange", 3.0, 9),
          ("South", "Orange", 6.0, 18),
          ("East",  "Milk",   5.0, 5)
        ).toDF("store", "prod", "amt", "units")

        sparkSession.udf.register("gm", new GeometricMean)

        // Create an instance of UDAF GeometricMean.
        val maxQualityUdaf = new MaxQualityFastQ

        // Show the geometric mean of values of column "id".
        val aggDf = rawDf
          .groupBy("store", "prod")
          .agg(maxQualityUdaf(col("amt")).as("MaxQualityFastQ"))

        aggDf.printSchema
        aggDf.show(false)

      }
    }
