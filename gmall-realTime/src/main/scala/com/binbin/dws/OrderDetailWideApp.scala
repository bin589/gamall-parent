package com.binbin.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.binbin.bean.{OrderDetail, OrderInfo}
import com.binbin.dwd.OrderDetailWide
import com.binbin.util.{MySparkUtils, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author libin
  * @create 2020-06-26 3:34 下午
  */
object OrderDetailWideApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("OrderDetailWideApp").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    val orderInfoTopic = "DWD_ORDER_INFO"
    val orderInfoGroup = "dws_order_info"
    val orderDetailTopic = "DWD_ORDER_DETAIL"
    val orderDetailGroup = "dws_order_detail"

    val kafkaOrderInfoDS: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, orderInfoTopic, orderInfoGroup)
    var orderInfoRanges: Array[OffsetRange] = null

    val orderInfoStrDS: DStream[ConsumerRecord[String, String]] =
      kafkaOrderInfoDS.transform { rdd =>
        orderInfoRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val orderInfoDS: DStream[OrderInfo] = orderInfoStrDS.map { record =>
      val jsonStr: String = record.value()
      val orderInfo: OrderInfo =
        JSON.parseObject(jsonStr, classOf[OrderInfo])
      orderInfo
    }

    val kafkaOrderDetailDS: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, orderDetailTopic, orderDetailGroup)
    var orderDetailRanges: Array[OffsetRange] = null

    val orderDetailStrDS: DStream[ConsumerRecord[String, String]] =
      kafkaOrderDetailDS.transform { rdd =>
        orderDetailRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val orderDetailDS: DStream[OrderDetail] = orderDetailStrDS.map { record =>
      val jsonStr: String = record.value()
      val orderDetail: OrderDetail =
        JSON.parseObject(jsonStr, classOf[OrderDetail])
      orderDetail
    }

    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDS.map {
      info =>
        (info.id, info)
    }
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] =
      orderDetailDS.map { detail =>
        (detail.order_id, detail)
      }

    // 开窗
    val orderInfoWinDstream: DStream[(Long, OrderInfo)] =
      orderInfoWithKeyDstream.window(Seconds(6), Seconds(3))
    val orderDetailWinDstream: DStream[(Long, OrderDetail)] =
      orderDetailWithKeyDstream.window(Seconds(6), Seconds(3))

//    join
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] =
      orderInfoWinDstream.join(orderDetailWinDstream)

    // 去重
    val orderJoinNewDS: DStream[(Long, (OrderInfo, OrderDetail))] =
      orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val key = "order_join_keys"
        val orderJoinNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]
        for ((orderId, (orderInfo, orderDetail)) <- orderJoinedTupleItr) {
          val isNew: lang.Long = jedisClient.sadd(key, orderDetail.id.toString)
          if (isNew == 1L) {
            orderJoinNewList.append((orderId, (orderInfo, orderDetail)))
          }
        }
        jedisClient.close()
        orderJoinNewList.toIterator
      }

    val orderDetailWideDS: DStream[OrderDetailWide] = orderJoinNewDS.map {
      case (orderId, (orderInfo, orderDetail)) =>
        val detailWide = new OrderDetailWide(orderInfo, orderDetail)
        detailWide
    }
//    sku_price * sku_num/final_total_amount(实付) * original_total_amount(原始金额)

    val orderDetailWideWithFinalDetailAmountDS: DStream[OrderDetailWide] =
      orderDetailWideDS.mapPartitions { orderDetailWideItr =>
        val orderDetailWideList: List[OrderDetailWide] =
          orderDetailWideItr.toList
        val jedisClient: Jedis = RedisUtil.getJedisClient

        val originalTotalAmountField = "originalTotalAmount"
        val finalDetailAmountTotalField = "finalDetailAmountTotal"

        for (orderDetailWide <- orderDetailWideList) {

          val orderId: Long = orderDetailWide.order_id
          val skuNum: Long = orderDetailWide.sku_num
          val skuPrice: Double = orderDetailWide.sku_price
          val finalTotalAmount: Double = orderDetailWide.final_total_amount
          val originalTotalAmount: Double =
            orderDetailWide.original_total_amount
          val skuAmount: Double = skuNum * skuPrice

          val finalDetailAmount: Double = 0d

          // 如何判断是最后一笔
          // 如果 该条明细 （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）

          val key = "order_split_amount:" + orderId

          // 金额累计
          var originalTotalAmountFieldByRedis: Double = 0D
          val originalTotalAmountFieldByRedisStr: String =
            jedisClient.hget(key, originalTotalAmountField)

          if (originalTotalAmountFieldByRedisStr != null && originalTotalAmountFieldByRedisStr.length > 0) {
            originalTotalAmountFieldByRedis =
              originalTotalAmountFieldByRedisStr.toDouble
          }

          // 分摊金额累计
          val finalDetailAmountTotalByRedisStr: String =
            jedisClient.hget(key, finalDetailAmountTotalField)

          var finalDetailAmountTotalByRedis: Double = 0l
          if (finalDetailAmountTotalByRedisStr != null && finalDetailAmountTotalByRedisStr.length > 0) {
            finalDetailAmountTotalByRedis =
              finalDetailAmountTotalByRedisStr.toDouble
          }

          // 判断是不是最后一笔
          if (originalTotalAmountFieldByRedis + skuAmount == originalTotalAmountField) {
            // 最后一单

            val finalDetailAmount = finalTotalAmount - finalDetailAmountTotalByRedis

          } else {

            val finalDetailAmount
              : Double = skuAmount / finalTotalAmount * originalTotalAmount
              .formatted("%.2f")
              .toDouble

            // 分摊金额累计
            jedisClient.hset(
              key,
              finalDetailAmountTotalField,
              (finalDetailAmount + finalDetailAmountTotalByRedis).toString
            )

            // 原价累计
            jedisClient.hset(
              key,
              originalTotalAmountField,
              (skuAmount + originalTotalAmountFieldByRedis).toString
            )

            jedisClient.expire(key, 60 * 60)
          }
          orderDetailWide.final_detail_amount = finalDetailAmount

        }

        orderDetailWideList.iterator
      }

    ssc.start()
    ssc.awaitTermination()
  }

}
