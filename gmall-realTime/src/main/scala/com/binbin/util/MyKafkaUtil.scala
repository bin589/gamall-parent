package com.binbin.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  KafkaUtils,
  LocationStrategies
}

import scala.collection.mutable

object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  var kafkaParam: mutable.Map[String, Object] = mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[
      StringDeserializer
    ],
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall_consumer_group",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
  )

  def getKafkaStream(topic: String, ssc: StreamingContext) = {
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  def getKafkaStream(
    topic: String,
    ssc: StreamingContext,
    groupId: String
  ): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  def getKafkaStream(
    topic: String,
    ssc: StreamingContext,
    offsets: Map[TopicPartition, Long],
    groupId: String
  ): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies
        .Subscribe[String, String](Array(topic), kafkaParam, offsets)
    )
    dStream
  }

}
