package com.hamshif.wielder.pipelines.kafka_ingestion

import com.hamshif.wielder.wild.{CommonDictionary, DatalakeConfig, FsUtil}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.DateTime

/**
  * This ETL uses a direct stream completely circumventing use of Zookeeper or Kafka side group offset management
  * TODO make sure partitions don't do duplicate work i.e. divide the offset range
  */

object KafkaActivityIngestor1 extends LegacyKafkaArgParser with CommonDictionary with FsUtil with Logging {

  val MAX_EMPTY_MICRO_BATCHES = 10

  val PARTITIONS = 2


  def main (args: Array[String]): Unit = {

    val conf = getConf(args)
    val specificConf = getSpecificConf(args)

    KafkaActivityIngestor1.start(conf, specificConf)
  }


  def start (conf: DatalakeConfig, specificConf: LegacyKafkaConfig): Unit = {

    val env = conf.env

    val runtimeEnv = conf.runtimeEnv

    val bootstrapServers = specificConf.bootsrapServers

    val topicIds = conf.kafkaTopics.split(",")
    val topicBucketName = f"${conf.bucketName}"

    val baseAccountPath = s"${conf.fsPrefix}${topicBucketName}"


    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master(conf.sparkMaster)
      .config("spark.network.timeout", "40000")

      .config("spark.network.timeout", "40000")
      .config("spark.streaming.kafka.maxRatePerPartition", "100")
      .config("request.timeout.ms", "30000")
      .config("session.timeout.ms", "3000")

      //      .config("bootstrap.servers", bootstrapServers)
      //      .config("key.deserializer", classOf[StringDeserializer].getCanonicalName)
      //      .config("value.deserializer", classOf[StringDeserializer].getCanonicalName)
      //      .config("auto.offset.reset", "earliest")
      //      .config("enable.auto.commit", "true")
      //
      //      .config("auto.commit.interval.ms", "1000")

      .getOrCreate()


    val sourcePath = new Path(baseAccountPath)
    val fs = sourcePath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)

    val INGEST_KAFKA = "ingest_kafka" // TODO get this from dictionary some maven intellij crap
    var offsetState = getOrCreateOffset(fs, baseAccountPath, INGEST_KAFKA)
    logInfo(s"Offset: $offsetState")

    if(offsetState.isLocked){

      logInfo("Offset is locked TODO add supervision in logs")

      return
    }

    println("munchkins: ")
    topicIds.map(munchkin => println(s"munchkin: $munchkin, "))

    println()



    val kafkaParams = Map(

      "bootstrap.servers" -> bootstrapServers,

      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "smallest",
//      "auto.offset.reset" -> "largest",

      "request.timeout.ms" -> "30000",
      "session.timeout.ms" -> "3000"
    )


    val sc = sparkSession.sparkContext

    println(s"\nspark conf:\n")

    sc.getConf.getAll.map(prop => {
      println(s"property:     ${prop}\n")
    })


    val streamingContext = new StreamingContext(sc, Seconds(10))



    val topics: Set[String] = topicIds.map(munchkin => s"${specificConf.topicsPrefix}$munchkin").toSet //++ devTopics

    val m = (0 until PARTITIONS).foldLeft(Map[TopicAndPartition, Long]())((acc, b) => {

      acc + ((TopicAndPartition(topics.head, b), offsetState.offset))
    })


    val messageHandler: MessageAndMetadata[String, String] => (String, String) =
      (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

    val kafkaStream = specificConf.useOffsets match {
      case true =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, m, messageHandler)
      case _ =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    }


    var count = 0

    kafkaStream.foreachRDD(rdd => {

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      println("HI Caramba! offset ranges are:")

      val munchkinOffsetMap = offsetRanges.foldLeft(Map[String, Long]())((b, o) => {

        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

        val top = o.topic.replace(specificConf.topicsPrefix, "")

        b + (top -> Math.max(b.get(top).getOrElse(0L), o.untilOffset))
      })

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
        .withColumnRenamed("_2", KEY_MESSAGE)


      if(! cleansed.head(1).isEmpty) {

        offsetState = getOrCreateOffset(fs, baseAccountPath, INGEST_KAFKA)

        val highestOffset = munchkinOffsetMap.get(topicIds.head).getOrElse(0L)

        val lockedOffsets = lockOffsets(fs, offsetState.fileStatus, s"${offsetState.basePath}/${highestOffset}")

        count = 0
        cleansed.show()

        val sinkDirPath = s"${conf.fsPrefix}${topicBucketName}/$RAW/$KAFKA/$fullDate"

        println(s"Dataframe head has values writing them to:\n$sinkDirPath")

        cleansed
          .write
          //          .partitionBy(KEY_MUNCHKIN_ID, "landing", "kafka", KEY_DATE)
          .mode(SaveMode.Append)
          //          TODO rename from sync to sink
          .format(conf.syncType)
          .save(sinkDirPath)

        val newOffset = unlockOffset(fs, lockedOffsets._1, lockedOffsets._2)
        println(s"Write succeeded changing highestOffset to: $newOffset")
      }
      else {

        count = count + 1
        println(s"The last ${count} Dataframe heads were empty! not writing this batch")

        if(count > MAX_EMPTY_MICRO_BATCHES){

          println(s"The last ${count} Dataframe heads were empty! exceeded MAX_EMPTY_MICRO_BATCHES: $MAX_EMPTY_MICRO_BATCHES\nTerminating job")
          streamingContext.stop(stopSparkContext = true)
        }
      }

    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
