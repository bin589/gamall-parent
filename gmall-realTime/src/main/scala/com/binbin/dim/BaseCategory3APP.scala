package com.binbin.dim

import com.alibaba.fastjson.JSON
import com.binbin.bean.BaseCategory3
import com.binbin.util.{MyConstant, OffsetManager, MySparkUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 保存三级分类到hbase维度表
  *
  *@author libin
  *@create 2020-06-25 10:15 上午
  */
object BaseCategory3APP {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("BaseCategory3APP").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topicName = "ODS_BASE_CATEGORY3"
    val groupId = "base_category3"

    val inputDStream: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, topicName, groupId)

    var offsetRanges: Array[OffsetRange] = null
    val dStream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    import org.apache.phoenix.spark._
    dStream.foreachRDD { rdd =>
      val baseCategoryRdd: RDD[BaseCategory3] = rdd.map { record =>
        val baseCategory: BaseCategory3 =
          JSON.parseObject(record.value(), classOf[BaseCategory3])
        baseCategory
      }
      baseCategoryRdd.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_base_category3",
        Seq("ID", "NAME", "CATEGORY2_ID"),
        new Configuration,
        Some(MyConstant.ZK_URL)
      )

      // 保存offset
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
