package com.binbin.realTime

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.DauInfo
import com.binbin.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author libin
  * @create 2020-06-17 5:10 下午
  */
object DauApp {
  private val GMALL_STARTUP_0105 = "GMALL_STARTUP_0105"
  private val GMALL_EVENT_0105 = "GMALL_EVENT_0105"

  private val GROUP_ID = "GROUP_NAME_0105"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf =
      new SparkConf()
        .setAppName("dau_app")
        .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topicName = GMALL_STARTUP_0105
    val groupId = GROUP_ID

    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    // 获取offset
    val kafkaOffsetMap: Map[TopicPartition, Long] =
      OffsetManager.getOffset(topicName, groupId)

    if (kafkaOffsetMap.isEmpty) {
      recordInputStream = MyKafkaUtil.getKafkaStream(topicName, ssc)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      recordInputStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map {
      record =>
        {
          val jsonString: String = record.value()
          val jsonObj: JSONObject = JSON.parseObject(jsonString)
          val ts: Long = jsonObj.getLong("ts")
          val datehourString: String =
            new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
          val dateHour: Array[String] = datehourString.split(" ")
          jsonObj.put("dt", dateHour(0))
          jsonObj.put("hr", dateHour(1))
          jsonObj
        }
    }

    // 去重
    val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions {
      jsonObjItr =>
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val filterList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        val jsonList: List[JSONObject] = jsonObjItr.toList
        println("过滤前：" + jsonList.size)
        for (jsonObj <- jsonList) {
          val dt: String = jsonObj.getString("dt")
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
//          println(s"mid=${mid}")
          val dauKey: String = "dau:" + dt
          val isNew: Long = jedisClient.sadd(dauKey, mid)
          jedisClient.expire(dauKey, 3600 * 24)
          if (isNew == 1l) {
            filterList += jsonObj
          }
        }
        jedisClient.close()
        println("过滤后：" + filterList.size)
        filterList.iterator
    }
    // 存放es
    filteredDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonItr =>
        val list: List[JSONObject] = jsonItr.toList
        val dauList: List[DauInfo] = list.map { jsonObj =>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          DauInfo(
            commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )
        }

        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.bulkDoc(dauList, "gmall_dau_info_" + dt)
      }
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
