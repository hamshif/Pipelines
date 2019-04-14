package com.hamshif.wielder.pipelines.kafka_ingestion

case class LegacyKafkaConfig (
                               bootsrapServers: String = "",
                               consumerGroupId: String = "",
                               zookeeperArgs: String = "",
                               topicsPrefix: String = "activities.",
                               useOffsets: Boolean = false
                             )
