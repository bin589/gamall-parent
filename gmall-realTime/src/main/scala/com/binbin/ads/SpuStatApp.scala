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

/** spu统计
  *
  * @author libin
  * @create 2020-07-01 9:40 上午
  */
object SpuStatApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setMaster("local[4]").setAppName("SpuStatApp")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topicName = "DWD_ORDER_WIDE_SPU"
    val groupId = "SpuStatApp"
    val offsetMap: Map[TopicPartition, Long] =
      OffsetManagerMySql.getOffset(topicName, groupId)

    var kafkaDs: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap.isEmpty) {
      kafkaDs = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
    } else {
      kafkaDs = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetMap, groupId)
    }

    var offsetRanges: Array[OffsetRange] = null
    val ds: DStream[ConsumerRecord[String, String]] = kafkaDs.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderWideDS: DStream[OrderDetailWide] = ds.map { orderRecord =>
      val orderJsonObj: String = orderRecord.value()
      println(orderJsonObj)
      val orderDetailWide: OrderDetailWide =
        JSON.parseObject(orderJsonObj, classOf[OrderDetailWide])
      orderDetailWide
    }

    val spuOrderWideDS: DStream[(String, Double)] = orderWideDS.map {
      orderWide =>
        (
          orderWide.spu_id + "_" + orderWide.spu_name,
          orderWide.final_total_amount
        )
    }

    val spuIdReduceDS: DStream[(String, Double)] =
      spuOrderWideDS.reduceByKey(_ + _)

    //  执行写入
    spuIdReduceDS.foreachRDD { rdd =>
      val amountArray: Array[(String, Double)] = rdd.collect()
      if (amountArray.nonEmpty) {
        DBs.setup()
        DB.localTx(implicit session => {
          // 保存spu信息
          amountArray.foreach {
            case (spu, finalTotalAmount) =>
              val spuArray: Array[String] = spu.split("_")

              SQL("insert into spu_amount_stat values (?,?,?,?)")
                .bind(
                  MyDateUtils.getNowYMDHMS(),
                  spuArray(0),
                  spuArray(1),
                  finalTotalAmount
                )
                .update()
                .apply()

          }

          // 保存offset
          offsetRanges.foreach { offset =>
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
