package com.binbin.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

/**
  * @author libin
  * @create 2020-06-25 10:02 上午
  */
object MySparkUtils {

  def getInputDStream(
    ssc: StreamingContext,
    topicName: String,
    groupId: String
  ): InputDStream[ConsumerRecord[String, String]] = {

    // 获取偏移量
    val offsetMap: Map[TopicPartition, Long] =
      OffsetManager.getOffset(topicName, groupId)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null;
    if (offsetMap.isEmpty) {
      kafkaDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
    } else {
      kafkaDStream =
        MyKafkaUtil.getKafkaStream(topicName, ssc, offsetMap, groupId)
    }
    kafkaDStream
  }
}
