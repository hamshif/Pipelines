    package com.hamshif.wielder.pipelines.snippets

    import org.apache.spark.sql._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._


    object TestJob7 {

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
          ("West",  "Apple",  2.0, 10),
          ("West",  "Apple",  3.0, 15),
          ("West",  "Orange", 5.0, 15),
          ("South", "Orange", 3.0, 9),
          ("South", "Orange", 6.0, 18),
          ("East",  "Milk",   5.0, 5)
        ).toDF("store", "prod", "amt", "units")


        rawDf.show(false)
        rawDf.printSchema

        val storeProdWindow = Window
          .partitionBy("store", "prod")

        val aggDf = rawDf
          .withColumn("max(amt)", max("amt").over(storeProdWindow))
          .withColumn("avg(units)", avg("units").over(storeProdWindow))

        aggDf.printSchema

        aggDf.show(false)
      }
    }
