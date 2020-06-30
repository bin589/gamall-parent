package com.binbin.ads

import com.alibaba.fastjson.JSON
import com.binbin.bean.OrderDetailWide
import com.binbin.util.{MyDateUtils, MyKafkaUtil, OffsetManagerMySql}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * @author libin
  * @create 2020-06-30 8:19 下午
  */
object TrademarkStatApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("TrademarkStatApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val topicName = "DWD_ORDER_WIDE"
    val groupId = "TrademarkStatApp"
    val kafkaOffsetMap: Map[TopicPartition, Long] =
      OffsetManagerMySql.getOffset(topicName, groupId)

    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      recordInputStream =
        MyKafkaUtil.getKafkaStream(topicName, ssc, kafkaOffsetMap, groupId)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      recordInputStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val orderWideDS: DStream[OrderDetailWide] = inputGetOffsetDstream.map {
      jsonObj =>
        val jsonStr: String = jsonObj.value()
        println(jsonStr)
        val wide: OrderDetailWide =
          JSON.parseObject(jsonStr, classOf[OrderDetailWide])
        wide
    }
    val amountWithTmDstream: DStream[(String, Double)] = orderWideDS.map {
      orderWide =>
        (
          orderWide.tm_id + ":" + orderWide.tm_name,
          orderWide.final_detail_amount
        )
    }
    val amountReduceDS: DStream[(String, Double)] =
      amountWithTmDstream.reduceByKey(_ + _)

//    存储本地事务方式
    amountReduceDS.foreachRDD { rdd =>
      val amountArray: Array[(String, Double)] = rdd.collect()
      if (amountArray != null && amountArray.size > 0) {
        DBs.setup()
        DB.localTx(implicit session => {
          for ((tm, amount) <- amountArray) {
            ///写数据库
            val tmArr: Array[String] = tm.split(":")
            val tmId = tmArr(0)
            val tmName = tmArr(1)
            println("数据写入 执行")
            val now: String = MyDateUtils.getNowYMDHMS()
            SQL("insert into trademark_amount_stat values (?,?,?,?)")
              .bind(now, tmId, tmName, amount)
              .update()
              .apply()
          }
          // 提交偏移量
          for (offset <- offsetRanges) {
            val partitionId: Int = offset.partition
            val untilOffset: Long = offset.untilOffset
            SQL(
              "REPLACE INTO  offset(group_id,topic,partition_id,topic_offset)  VALUES(?,?,?,?) "
            ).bind(groupId, topicName, partitionId, untilOffset)
              .update()
              .apply()
          }
        })
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
