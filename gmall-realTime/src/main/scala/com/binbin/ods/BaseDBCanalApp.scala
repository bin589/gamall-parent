package com.binbin.ods

import com.alibaba.fastjson.{JSON, JSONArray}
import com.binbin.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author libin
  * @create 2020-06-22 3:24 下午
  */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf =
      new SparkConf().setAppName("base_db_canal_app").setMaster("local[4]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName = "GMALL_DB_TO_KAFKA"
    val groupId = "base_db_canal_group"
    val kafkaOffsetMap: Map[TopicPartition, Long] =
      OffsetManager.getOffset(topicName, groupId)

    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null;
    if (kafkaOffsetMap.isEmpty == null) {
      inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc)
    } else {
      inputDStream =
        MyKafkaUtil.getKafkaStream(topicName, ssc, kafkaOffsetMap, groupId)
    }

    var offsetRanges = Array.empty[OffsetRange]
    val recordInputStream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val jsonObjStream = recordInputStream.map { rdd =>
      val jsonString = rdd.value()
      println(jsonString)
      val jSONObject = JSON.parseObject(jsonString)
      jSONObject
    }

    // 推回kafka
    jsonObjStream.foreachRDD { rdd =>
      rdd.foreach { jsonObj =>
        val dataArray: JSONArray = jsonObj.getJSONArray("data")
        val tableName: String = jsonObj.getString("table")
        val topic: String = "ODS_" + tableName.toUpperCase

//        for (dataJson <- dataArray) {
//          val msg: String = jsonObj.toString
//          MyKafkaSink.send(topic, msg)
//        }

      }
    }

    OffsetManager.saveOffset(topicName, groupId, offsetRanges)

//    jsonObjStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
