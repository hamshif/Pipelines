package com.hamshif.wielder.pipelines.kafka_ingestion

import com.hamshif.wielder.wild.{CommonDictionary, DatalakeConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * This ETL uses an indirect stream which tries to use of Zookeeper or Kafka side group offset management.
  * Notice that in some cases zookeeper CSV needs /{sub directory}
  * If Zookeeper for group offset management
  *
  * TODO make sure partitions don't do duplicate work i.e. divide the offset range
  */

object KafkaActivityIngestor extends LegacyKafkaArgParser with CommonDictionary with Logging {

  val MAX_EMPTY_MICRO_BATCHES = 10


  def main (args: Array[String]): Unit = {

    val conf = getConf(args)
    val specificConf = getSpecificConf(args)

    KafkaActivityIngestor.start(conf, specificConf)
  }


  def start (conf: DatalakeConfig, specificConf: LegacyKafkaConfig): Unit = {

    val env = conf.env

    val runtimeEnv = conf.runtimeEnv

    val bootstrapServers = specificConf.bootsrapServers
    val zkArgs = specificConf.zookeeperArgs

    println(
      s"\nstarted DataLakeLeadActivityProcessor job\n" +
        s"env: $env\n" +
        s"runtimeEnv: $runtimeEnv\n" +
        s"bootstrapServers: $bootstrapServers\n" +
        s"zookeeperArgs args: $zkArgs\n" +
        s"munchkinIds:"
    )

    val bucketName = conf.bucketName

//    Disregard these are specific kafka oozie-flow configuration and can be deleted
    val comp = "gid"
    val serviceId = "sjqe-gid-service"
    val clusterName = "sjqe-gid-1"


    println()

    val consumerGroupId = specificConf.consumerGroupId

    println(s"consumerGroupId: $consumerGroupId")



    println(s"\nspark master:          ${conf.sparkMaster}\n")
    println(s"\nconsumerGroupId: $consumerGroupId\n")
    println(s"\nzkArgs:          $zkArgs\n")


    val sparkConf = new SparkConf()
//      .setAppName(this.getClass.getSimpleName.replace("$", "")
      .setAppName(comp)
      .setMaster(conf.sparkMaster)

      .set("spark.network.timeout", "40000")

      .set("kafka.root", "\\kafka")

      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("bootstrap.servers", bootstrapServers)
      .set("key.deserializer", classOf[StringDeserializer].getCanonicalName)
      .set("value.deserializer", classOf[StringDeserializer].getCanonicalName)
      .set("group.id", consumerGroupId)
      .set("auto.offset.reset", "earliest")

      //      enabling watermarking upon success
      .set("enable.auto.commit", "false")
      //      .set("auto.commit.interval.ms", "1000")

      .set("request.timeout.ms", "30000")
      .set("session.timeout.ms", "3000")

      .set("zookeeper.hosts", zkArgs)

      .set("envName", env.toString.toUpperCase)
      .set("platforminsight.schema.registry.url", s"http://$env-schemareg-ws.marketo.org")
      .set("serviceName", serviceId)
      .set("dcName", "SJ")
      .set("rml.enabled", "false")
      .set("platforminsight.bootstrap.servers", bootstrapServers)
      .set("enablePush", "true")
      .set("multitenant.errorhandler.suspensionTime", "180")
      .set("multitenant.ratecontroller", "PendingBatchRateController")
      .set("spark.streaming.batchSize", "5")
      .set("multitenant.ratecontroller.pendingbatchcontroller.maxPendingBatches", "0")
      .set("rml.tsdb.url", s"http://sj$env-tsdb-vip.marketo.org")
      .set("clusterName", clusterName)
      .set("streaming.application", comp)


    val sc = new SparkContext(sparkConf)

    println(s"\nspark conf:\n")

    sparkConf.getAll.map(prop => {
      println(s"property:     ${prop}\n")
    })

    val streamingContext = new StreamingContext(sc, Seconds(10))

    val topics: Array[String] = conf.kafkaTopics.split(",")
    val numThreads = "5"

    val topicMap = topics.map((_, numThreads.toInt)).toMap

    var count = 0

    val kafkaStream = KafkaUtils.createStream(streamingContext, zkArgs, consumerGroupId, topicMap)

    kafkaStream.foreachRDD(rdd => {

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val date = DateTime.now

      val fullDate = date.toString(DATE_FORMAT)

      val year = date.toString("yyyy")
      val month = date.toString("MM")
      val day = date.toString("dd")

      val df = rdd.toDF()
//        .withColumn(KEY_MUNCHKIN_ID, toMunchkinUDF(col("_1")))
      //Option multi munchkin
      //        .withColumn("kafka", lit("kafka"))
      //        .withColumn("landing", lit("landing"))
      //        .withColumn(KEY_DATE, lit(date))

      //      df.show()
      //    TODO try to get rid of cleansing it's better to bring raw kafka
      val cleansed = df
        .select(col("_2"))
//        .withColumnRenamed("_2", KEY_MESSAGE)


      if(! cleansed.head(1).isEmpty) {

        count = 0

        cleansed.show()

        val sinkDirPath = s"${conf.fsPrefix}${bucketName}/$RAW/$KAFKA/$fullDate"

        println(s"Dataframe head has values writing them to:\n$sinkDirPath")

        cleansed
          .withWatermark("eventTime", "10 minutes")
          .write
          //          .partitionBy(KEY_MUNCHKIN_ID, "landing", "kafka", KEY_DATE)
          .mode(SaveMode.Append)
          //          TODO rename from sync to sink
          .format(conf.syncType)
          .save(sinkDirPath)


      }
      else {

        count = count + 1

        println(s"The last ${count} Dataframe heads were empty! not writing this batch")

        if(count > MAX_EMPTY_MICRO_BATCHES){

          println(s"The last ${count} Dataframe heads were empty! exceded MAX_EMPTY_MICRO_BATCHES: $MAX_EMPTY_MICRO_BATCHES\nTerminating job")
          streamingContext.stop(stopSparkContext = true)
        }
      }

      //Remove from cache
//      cleansed.unpersist()

    })

    streamingContext.start()
    streamingContext.awaitTermination()
//    streamingContext.
  }
}
