package com.binbin.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.{OrderDetail, SkuInfo}
import com.binbin.util._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
  * @author libin
  * @create 2020-06-25 9:43 上午
  */
object OrderDetailApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("OrderDetailApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 获取偏移量
    val topicName = "ODS_ORDER_DETAIL"
    val groupId = "order_detail"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, topicName, groupId)

    var offsetRanges: Array[OffsetRange] = null
    val dStream: DStream[ConsumerRecord[String, String]] =
      kafkaDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val orderDetailDS: DStream[OrderDetail] = dStream.map { record =>
      val objStr: String = record.value()
      val orderDetail: OrderDetail =
        JSON.parseObject(objStr, classOf[OrderDetail])
      orderDetail
    }
    // 合并维表数据
    val orderDetailWithSkuDS: DStream[OrderDetail] = orderDetailDS.transform {
      rdd =>
        val skuIdList: Set[Long] = rdd.map(_.sku_id).collect().toSet
        if (skuIdList.isEmpty) {
          rdd
        } else {
          val sql =
            s"select ID,SPU_NAME,CATEGORY3_ID,CATEGORY3_NAME,TM_ID,TM_NAME " +
              s"from ${MyConstant.HBASE_TABLE_PRE}_sku_info " +
              s"where id  in ('${skuIdList.mkString("','")}')"

          val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(sql)

          val skuInfoMap: Map[String, SkuInfo] = jSONObjects.map { jsonObj =>
            val skuInfo: SkuInfo = JSON.toJavaObject(jsonObj, classOf[SkuInfo])
            (skuInfo.id, skuInfo)
          }.toMap

          val skuInfoBC: Broadcast[Map[String, SkuInfo]] =
            ssc.sparkContext.broadcast(skuInfoMap)
          val orderDetailRdd: RDD[OrderDetail] = rdd.map { orderDetail =>
            val skuMap: Map[String, SkuInfo] = skuInfoBC.value
            val skuInfo: SkuInfo =
              skuMap.getOrElse(orderDetail.sku_id.toString, null)
            if (skuInfo != null) {
//              orderDetail.spu_id=skuInfo.spu_id.toLong
              orderDetail.spu_name = skuInfo.spu_name
              orderDetail.tm_id = skuInfo.tm_id.toLong
              orderDetail.tm_name = skuInfo.tm_name
              orderDetail.category3_id = skuInfo.category3_id.toLong
              orderDetail.category3_name = skuInfo.category3_name
            }
            orderDetail
          }
          orderDetailRdd
        }

    }
    orderDetailWithSkuDS.foreachRDD { orderDetailRDD =>
      // 发送到kafka
      orderDetailRDD.foreachPartition { orderDetailIter =>
        val orderDetailList: List[OrderDetail] = orderDetailIter.toList
        for (orderDetail <- orderDetailList) {
          val orderDetailsStr: String =
            JSON.toJSONString(orderDetail, new SerializeConfig(true))
          println(s"DWD_ORDER_DETAIL=>${orderDetailsStr}")
          MyKafkaSink.send(
            "DWD_ORDER_DETAIL",
            orderDetail.id.toString,
            orderDetailsStr
          )
        }
      }

      //  保存到hbase
      //  TODO 测试先注释
      orderDetailRDD.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_order_detail",
        Seq(
          "ID",
          "ORDER_ID",
          "SKU_ID",
          "ORDER_PRICE",
          "SKU_NUM",
          "SKU_NAME",
          "CREATE_TIME",
          "SPU_ID",
          "SPU_NAME",
          "TM_ID",
          "TM_NAME",
          "CATEGORY3_ID",
          "CATEGORY3_NAME"
        ),
        new Configuration,
        Some(MyConstant.ZK_URL)
      )
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
