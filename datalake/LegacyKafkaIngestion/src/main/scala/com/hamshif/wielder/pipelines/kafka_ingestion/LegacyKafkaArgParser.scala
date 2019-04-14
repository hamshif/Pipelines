package com.hamshif.wielder.pipelines.kafka_ingestion

import com.hamshif.wielder.wild.DatalakeArgParser

trait LegacyKafkaArgParser extends DatalakeArgParser {

  def getSpecificConf(args: Array[String]): LegacyKafkaConfig = {

//    args.map(arg => println(s"arg: $arg"))

    sParser.parse(args, LegacyKafkaConfig()) match {

      case Some(config) => config
      case None => throw new Exception("bad config")
    }
  }

  val sParser = new scopt.OptionParser[LegacyKafkaConfig](this.getClass.getName) {

    override def errorOnUnknownArgument = false

    head("scopt", "3.x")

    opt[String]( "bootstrap")
      .action((x, c) => c.copy(bootsrapServers = x))
      .text("Kafka Bootstrap connection CSV string")

    opt[String]( "groupid")
      .action((x, c) => c.copy(consumerGroupId = x))
      .text("Kafka group Id for Kafka side offset management")

    opt[String]('z', "zookeeper")
      .action((x, c) => c.copy(zookeeperArgs = x))
      .text("Zookeeper connection CSV string leads by zookeeperArgs URLs")

    opt[Boolean]( "offsets")
      .action((x, c) => c.copy(useOffsets = x))
      .text("Boolean defaults to false Use Offsets or just read from the beginning")
  }
}
