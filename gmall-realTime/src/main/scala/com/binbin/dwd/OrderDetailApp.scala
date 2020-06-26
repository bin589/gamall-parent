package com.binbin.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.{OrderDetail, SkuInfo}
import com.binbin.util.{MyConstant, MySparkUtils, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author libin
  * @create 2020-06-25 9:43 上午
  */
object OrderDetailApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("OrderDetailApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

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

    val orderDetailWithSkuDS: DStream[OrderDetail] = orderDetailDS.transform {
      rdd =>
//        rdd.cache()
        val skuIdList: Array[Long] = rdd.map(_.sku_id).collect()
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
    //  保存到hbase
    import org.apache.phoenix.spark._
    orderDetailWithSkuDS.foreachRDD { rdd =>
      rdd.saveToPhoenix(
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
    // 合并维表数据
    // 品牌 分类 spu  作业
//    var spu_id: Long,
//    var tm_id: Long,
//    var category3_id: Long,
//    var spu_name: String,
//    var tm_name: String,
//    var category3_name: String

    ssc.start()
    ssc.awaitTermination()

  }
}
