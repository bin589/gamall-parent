package com.binbin.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  * @author libin
  * @create 2020-06-19 4:14 下午
  */
object OffsetManagerMySql {

  def getOffset(topicName: String, groupId: String) = {
    val sql = "SELECT partition_id , topic_offset FROM offset WHERE topic='" + topicName + "' AND group_id='" + groupId + "'"
    val partitionOffsetList: List[JSONObject] = MysqlUtil.queryList(sql)

    val topicOffsetMap: Map[TopicPartition, Long] = partitionOffsetList.map {
      josnObj =>
        (
          new TopicPartition(topicName, josnObj.getIntValue("partition_id")),
          josnObj.getLongValue("topic_offset")
        )
    }.toMap

    topicOffsetMap

  }

}
