package com.binbin.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author libin
  * @create 2020-06-22 5:06 下午
  */
object BaseDbMaxwell {

  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setAppName("base_db_maxwell").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    val topicName = "ODS_DB_GMALL2020_M"
    val groupId = "base_db_maxwel_group"
    val offsetMap: Map[TopicPartition, Long] =
      OffsetManager.getOffset(topicName, groupId)

    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null;
    if (offsetMap.isEmpty) {
      inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc)
    } else {
      inputDStream =
        MyKafkaUtil.getKafkaStream(topicName, ssc, offsetMap, groupId);
    }

    var offsetRanges = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
        rdd
      }

    val jsonObjDStream: DStream[JSONObject] = inputGetOffsetDstream.map {
      record =>
        val jsonString = record.value()
        val jSONObject = JSON.parseObject(jsonString)
        jSONObject
    }

    jsonObjDStream.foreachRDD { rdd =>
      // 推回kafka
      rdd.foreach { jsonObj =>
        if (!jsonObj
              .getJSONObject("data")
              .isEmpty) {

          val jsonString = jsonObj.getString("data")
          val tableName: String = jsonObj.getString("table")
          if (tableName == "order_info" || tableName == "order_detail") {
            val topic = "ODS_" + tableName.toUpperCase
            println(s"topic=>${topic}==>${jsonString}")
            MyKafkaSink.send(topic, jsonString) //非幂等的操作 可能会导致数据重复
          }

        }
      }

      OffsetManager.saveOffset(topicName, groupId, offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
