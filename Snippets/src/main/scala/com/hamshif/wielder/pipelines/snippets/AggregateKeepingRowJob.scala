    package com.hamshif.wielder.pipelines.snippets

    import org.apache.spark.sql._
    import org.apache.spark.sql.functions._


    object AggregateKeepingRowJob {

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
          (1L, "Moe",  "Slap",  2.0, 18),
          (2L, "Larry",  "Spank",  3.0, 15),
          (3L, "Curly",  "Twist", 5.0, 15),
          (4L, "Laurel", "Whimper", 3.0, 15),
          (5L, "Hardy", "Laugh", 6.0, 15),
          (6L, "Charley",  "Ignore",   5.0, 5)
        ).toDF("id", "name", "requisite", "money", "age")

        rawDf.show(false)
        rawDf.printSchema

        val maxAgeUdaf = new KeepRowWithMaxAge

        val aggDf = rawDf
          .groupBy("age")
          .agg(
            count("id"),
            max(col("money")),
            maxAgeUdaf(
              col("id"),
              col("name"),
              col("requisite"),
              col("money"),
              col("age")).as("KeepRowWithMaxAge")
          )

        aggDf.printSchema
        aggDf.show(false)

      }


    }
