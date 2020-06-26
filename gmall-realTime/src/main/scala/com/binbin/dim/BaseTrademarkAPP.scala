package com.binbin.dim

import com.alibaba.fastjson.JSON
import com.binbin.bean.BaseTrademark
import com.binbin.util.{MyConstant, OffsetManager, MySparkUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 保存品牌到hbase维度表
  *
  * @author libin
  * @create 2020-06-25 10:17 上午
  */
object BaseTrademarkAPP {
  def main(args: Array[String]): Unit = {

    val topicName: String = "ODS_BASE_TRADEMARK"
    val groupId: String = "base_trademark"
    val sparkConf: SparkConf =
      new SparkConf().setAppName("BaseTrademarkAPP").setMaster("local[4]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaInputDStream: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, topicName, groupId)

    var offsetRanges: Array[OffsetRange] = null;
    val dStream: DStream[ConsumerRecord[String, String]] =
      kafkaInputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    // 把数据保存到Hbase
    dStream.foreachRDD { rdd =>
      val trademarkRDD: RDD[BaseTrademark] = rdd.map { record =>
        val objStr: String = record.value()
        val baseTrademark: BaseTrademark =
          JSON.parseObject(objStr, classOf[BaseTrademark])
        baseTrademark
      }
      trademarkRDD.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_base_trademark",
        Seq("TM_ID", "TM_NAME"),
        new Configuration,
        Some(MyConstant.ZK_URL)
      )
      // 保存offsetRanges
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
