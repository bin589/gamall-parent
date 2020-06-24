package com.binbin.dim

import com.alibaba.fastjson.JSON
import com.binbin.bean.ProvinceInfo
import com.binbin.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author libin
  * @create 2020-06-23 7:16 下午
  */
object ProvinceApp {
  def main(args: Array[String]): Unit = {
    // 从kafka中读取省市区  然后写到base
    val sparkConf: SparkConf =
      new SparkConf().setAppName("ProvinceApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val topicName = "ODS_BASE_PROVINCE"
    val groupId = "base_province_group"

    val offsetRangesMap: Map[TopicPartition, Long] =
      OffsetManager.getOffset(topicName, groupId)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetRangesMap.isEmpty) {
      kafkaDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
    } else {
      kafkaDStream =
        MyKafkaUtil.getKafkaStream(topicName, ssc, offsetRangesMap, groupId)
    }
    var offsetRanges: Array[OffsetRange] = null;

    val recordDStream: DStream[ConsumerRecord[String, String]] =
      kafkaDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    //  写入到hbase
    import org.apache.phoenix.spark._
    recordDStream.foreachRDD { rdd =>
      val provinceInfoRdd: RDD[ProvinceInfo] = rdd.map { record =>
        val jsonObj: String = record.value()
        println(jsonObj)
        val provinceInfo: ProvinceInfo =
          JSON.parseObject(jsonObj, classOf[ProvinceInfo])
        provinceInfo
      }

      provinceInfoRdd.saveToPhoenix(
        "gmall0105_province_info",
        Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
