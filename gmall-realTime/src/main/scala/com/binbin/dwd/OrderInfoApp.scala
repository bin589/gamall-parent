package com.binbin.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.{OrderInfo, ProvinceInfo, UserInfo, UserState}
import com.binbin.util._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *@author libin
  *@create 2020-06-23 2:55 下午
  */
object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topicName = "ODS_ORDER_INFO"
    val groupId = "order_info_group"
    // kafka偏移
    val offsetRangeMap: Map[TopicPartition, Long] =
      OffsetManager.getOffset(topicName, groupId)

    var inputDStreamByKafka: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetRangeMap.isEmpty) {
      inputDStreamByKafka = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
    } else {
      inputDStreamByKafka =
        MyKafkaUtil.getKafkaStream(topicName, ssc, offsetRangeMap, groupId);
    }
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordInputStream: DStream[ConsumerRecord[String, String]] =
      inputDStreamByKafka.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    val orderInfoDS: DStream[OrderInfo] = recordInputStream.map { record =>
      val jsonStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      val timeArr: Array[String] = createTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      orderInfo
    }

    //查询hbase中用户状态
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDS
      .mapPartitions { itr =>
        val ordersInfoList: List[OrderInfo] = itr.toList
        val userIdList: List[Long] = ordersInfoList.map(_.user_id)

        if (userIdList.nonEmpty) {
          val sql =
            s"select * from ${MyConstant.HBASE_TABLE_PRE}_user_state where user_id in ('${userIdList
              .mkString("','")}')"
          val orderInfoObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
          val ifConsumedMap: Map[String, String] = orderInfoObjList.map {
            orderInfo =>
              (
                orderInfo.getString("USER_ID"),
                orderInfo.getString("IF_CONSUMED")
              )
          }.toMap

          for (orderInfo <- ordersInfoList) {
            val ifConsumed: String =
              ifConsumedMap.getOrElse(orderInfo.user_id.toString, null)
            if (ifConsumed != null && ifConsumed == "1") {
              orderInfo.if_first_order = "0";
            } else {
              orderInfo.if_first_order = "1";
            }
          }
        }
        ordersInfoList.toIterator
      }

    // 利用hbase  进行查询过滤 识别首单，只能进行跨批次的判断
    //  如果新用户在同一批次 多次下单 会造成 该批次该用户所有订单都识别为首单
    //  应该同一批次一个用户只有最早的订单 为首单 其他的单据为非首单
    val OrderInfoWithKeyDStream: DStream[(Long, OrderInfo)] =
      orderInfoWithFirstFlagDStream.map { orderInfo =>
        (orderInfo.user_id, orderInfo)
      }
    val orderInfoGroupByUidDStream: DStream[(Long, Iterable[OrderInfo])] =
      OrderInfoWithKeyDStream.groupByKey()

    val orderInfoWithFirstRealFlagDStream: DStream[OrderInfo] =
      orderInfoGroupByUidDStream.flatMap {
        case (userId, orderInfoItr) =>
          if (orderInfoItr.size > 1) {
            val orderInfoListSort: List[OrderInfo] =
              orderInfoItr.toList.sortWith { (o1, o2) =>
                o1.create_time < o2.create_time
              }
            for (i <- 1 until orderInfoListSort.size) {
              orderInfoListSort(i).if_first_order = "0"
            }
            orderInfoListSort
          } else {
            orderInfoItr.toList
          }
      }

    // 保存首单状态到hbase
    orderInfoWithFirstRealFlagDStream.cache()
    orderInfoWithFirstRealFlagDStream.foreachRDD { rdd =>
      val userStateRdd: RDD[UserState] = rdd
        .filter {
          _.if_first_order == "1"
        }
        .map { orderInfo =>
          val userState = UserState(orderInfo.user_id.toString, "1")
          userState
        }
      userStateRdd.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_user_state",
        Seq("USER_ID", "IF_CONSUMED"),
        new Configuration,
        Some(MyConstant.ZK_URL)
      )
    }

    // 优化 ： 因为传输量小  使用数据的占比大  可以考虑使用广播变量     查询hbase的次数会变小   分区越多效果越明显
    //利用driver进行查询 再利用广播变量进行分发
    // 维度数据合并
//    地址信息
    val orderInfoWithProvinceDS: DStream[OrderInfo] =
      orderInfoWithFirstRealFlagDStream.transform { rdd =>
        val sql =
          "select  id,name,area_code,iso_code from gmall0105_province_info"
        val provinceInfoObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        // 封装广播变量
        val provinceMap: Map[String, ProvinceInfo] = provinceInfoObjList.map {
          jsonObj =>
            val provinceInfo: ProvinceInfo = ProvinceInfo(
              jsonObj.getString("ID"),
              jsonObj.getString("NAME"),
              jsonObj.getString("AREA_CODE"),
              jsonObj.getString("ISO_CODE")
            )
            (provinceInfo.id, provinceInfo)

        }.toMap
        val provinceBC: Broadcast[Map[String, ProvinceInfo]] =
          ssc.sparkContext.broadcast(provinceMap)

        val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
          val provinceMap: Map[String, ProvinceInfo] = provinceBC.value
          val provinceInfoByOrder: ProvinceInfo =
            provinceMap.getOrElse(orderInfo.province_id.toString, null)
          if (provinceInfoByOrder != null) {
            orderInfo.province_name = provinceInfoByOrder.name
            orderInfo.province_area_code = provinceInfoByOrder.area_code
            orderInfo.province_iso_code = provinceInfoByOrder.iso_code
          }
          orderInfo
        }
        orderInfoWithProvinceRDD

      }

//    var user_age_group: String,
//    var user_gender: String)
    val orderInfoWithProvinceWithUserDS: DStream[OrderInfo] =
      orderInfoWithProvinceDS.transform { rdd =>
        val userIdSet: Set[Long] = rdd.map(_.user_id).collect().toSet
        if (userIdSet.isEmpty) {
          rdd
        } else {

          val sql = s"select id,user_age_group,user_gender " +
            s"from ${MyConstant.HBASE_TABLE_PRE}_user_info " +
            s"where id in (${userIdSet.mkString(",")})"

          val userInfoJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
          if (userInfoJsonList.isEmpty) {
            rdd
          } else {

            val userMap: Map[Long, UserInfo] = userInfoJsonList.map { useJson =>
              val userInfo: UserInfo = UserInfo(
                useJson.getLong("ID"),
                "",
                "",
                "",
                "",
                0,
                useJson.getInteger("USER_AGE_GROUP"),
                useJson.getString("USER_GENDER")
              )
              (userInfo.id, userInfo)

            }.toMap

            val userMapBC: Broadcast[Map[Long, UserInfo]] =
              ssc.sparkContext.broadcast(userMap)
            val orderInfoRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
              val userMap: Map[Long, UserInfo] = userMapBC.value
              val user: UserInfo = userMap.getOrElse(orderInfo.user_id, null)
              if (user != null) {
                orderInfo.user_gender = user.user_gender
                orderInfo.user_age_group = user.user_age_group
              }
              orderInfo
            }

            orderInfoRDD
          }
        }
      }

    // 保存数据到es和kafka
    orderInfoWithProvinceWithUserDS.foreachRDD { orderRdd =>
      orderRdd.foreachPartition { orderIter =>
        val ordersList: List[OrderInfo] = orderIter.toList

        val orderInfoWithIdList: List[(String, OrderInfo)] =
          ordersList.map(orderinfo => (orderinfo.id.toString, orderinfo))

        val dateString: String =
          new SimpleDateFormat(MyDateUtils.patternYMD).format(new Date())

        //  保存到es
//         TODO 测试先注释
//        MyEsUtil.bulkDoc(
        //          orderInfoWithIdList,
        //          "gmall0105_order_info_" + dateString
        //        )
        for (order <- ordersList) {
          val orderStr: String =
            JSON.toJSONString(order, new SerializeConfig(true))
          // 发送到kafka
          println(s"DWD_ORDER_INFO=>${orderStr}")
          MyKafkaSink.send("DWD_ORDER_INFO", order.id.toString, orderStr)
        }

      }

      // 保存到hbase
//       TODO 测试先注释
//      orderRdd.saveToPhoenix(
//        s"${MyConstant.HBASE_TABLE_PRE}_order_info",
//        Seq(
//          "ID",
//          "PROVINCE_ID",
//          "ORDER_STATUS",
//          "USER_ID",
//          "FINAL_TOTAL_AMOUNT",
//          "BENEFIT_REDUCE_AMOUNT",
//          "ORIGINAL_TOTAL_AMOUNT",
//          "FEIGHT_FEE",
//          "EXPIRE_TIME",
//          "CREATE_TIME",
//          "OPERATE_TIME",
//          "CREATE_DATE",
//          "CREATE_HOUR",
//          "IF_FIRST_ORDER",
//          "PROVINCE_NAME",
//          "PROVINCE_AREA_CODE",
//          "PROVINCE_ISO_CODE",
//          "USER_AGE_GROUP",
//          "USER_GENDER"
//        ),
//        new Configuration,
//        Some(MyConstant.ZK_URL)
//      )
      // 保存偏移量
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
  }
}
