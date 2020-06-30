package com.binbin.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
  * @author libin
  * @create 2020-06-19 4:14 下午
  */
object OffsetManager {

  def getOffset(topicName: String, groupId: String) = {
    val jedisClient: Jedis = RedisUtil.getJedisClient
    val offsetKey = s"offset:${topicName}:${groupId}"
    val offerSetMap: util.Map[String, String] = jedisClient.hgetAll(offsetKey)

    import scala.collection.JavaConversions._
    val kafkaOfferSetMap: Map[TopicPartition, Long] = offerSetMap.map {
      case (partitionId, offerSet) =>
        println(s"获取偏移量：partitionId=>${partitionId}==>offerSet==>${offerSet.toLong}")
        (new TopicPartition(topicName, partitionId.toInt), offerSet.toLong)
    }.toMap

    jedisClient.close()

    kafkaOfferSetMap
  }

  def saveOffset(topicName: String,
                 groupId: String,
                 offsetRanges: Array[OffsetRange]) = {

    val offsetKey = s"offset:${topicName}:${groupId}"

    val offerMap = new util.HashMap[String, String]();

    for (offset <- offsetRanges) {
      val partitionId: Int = offset.partition
      val offsetRange: Long = offset.untilOffset
      offerMap.put(partitionId.toString, offsetRange.toString)
      println(
        "写入分区：" + partitionId + ":" + offset.fromOffset + "-->" + offset.untilOffset
      )
    }

    if (!offerMap.isEmpty) {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      jedisClient.hmset(offsetKey, offerMap)
      jedisClient.close()
    }

  }
}
