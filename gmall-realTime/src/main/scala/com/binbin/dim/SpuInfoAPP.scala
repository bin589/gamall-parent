package com.binbin.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.{BaseCategory3, BaseTrademark, SpuInfo}
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
  * 保存spu到hbase维度表
  *
  *@author libin
  *@create 2020-06-25 10:14 上午
  */
object SpuInfoAPP {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setMaster("local[4]").setAppName("SpuInfoAPP")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicName = "ODS_SPU_INFO"
    val groupId = "spu_info"
    val inputDStream: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, topicName, groupId)

    var offsetRanges: Array[OffsetRange] = null
    val dStream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    val spuInfoDS: DStream[SpuInfo] = dStream.map { ds =>
      val objStr: String = ds.value()
      println(objStr)
      val spuInfo: SpuInfo = JSON.parseObject(objStr, classOf[SpuInfo])
      spuInfo

    }

    // 使用广播变量查数据
    // tm_name
    val spuInfoWithTmDS: DStream[SpuInfo] = spuInfoDS.transform { rdd =>
      // driver查询分类
      val tmSql =
        s"select tm_id, tm_name from ${MyConstant.HBASE_TABLE_PRE}_base_trademark";
      val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(tmSql)

      val tmMap: Map[String, BaseTrademark] = jSONObjects.map { jsonObj =>
        val baseTrademark: BaseTrademark = BaseTrademark(
          jsonObj.getString("TM_ID"),
          jsonObj.getString("TM_NAME")
        )
        (baseTrademark.tm_id, baseTrademark)
      }.toMap

      val tmBro: Broadcast[Map[String, BaseTrademark]] =
        ssc.sparkContext.broadcast(tmMap)

      val spuRdd: RDD[SpuInfo] = rdd.map { spuInfo =>
        val tmMap: Map[String, BaseTrademark] = tmBro.value
        val trademark: BaseTrademark = tmMap.getOrElse(spuInfo.tm_id, null)
        if (trademark != null) {
          spuInfo.tm_name = trademark.tm_name
        }
        spuInfo
      }

      spuRdd
    }

    //  category3_name
    val spuInfoWithTmDAndCate: DStream[SpuInfo] = spuInfoWithTmDS.transform {
      rdd =>
        val cateSql =
          s"select id, name from ${MyConstant.HBASE_TABLE_PRE}_base_category3";
        val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(cateSql)

        val categoryMap: Map[String, BaseCategory3] = jSONObjects.map {
          jsonObj =>

            val category: BaseCategory3 =
              BaseCategory3(
                jsonObj.getString("ID"),
                jsonObj.getString("NAME"),
                ""
              )
            (category.id, category)
        }.toMap

        val cateBC: Broadcast[Map[String, BaseCategory3]] =
          ssc.sparkContext.broadcast(categoryMap)

        val spuRdd: RDD[SpuInfo] = rdd.map { spuInfo =>
          val cateMap: Map[String, BaseCategory3] = cateBC.value
          val category: BaseCategory3 =
            cateMap.getOrElse(spuInfo.category3_id, null)
          if (category != null) {
            spuInfo.category3_name = category.name
          }
          spuInfo
        }
        spuRdd

    }

    // 保存到HBASE
    import org.apache.phoenix.spark._
    spuInfoWithTmDAndCate.foreachRDD { rdd =>
      rdd.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_spu_info",
        Seq(
          "ID",
          "SPU_NAME",
          "DESCRIPTION",
          "CATEGORY3_ID",
          "CATEGORY3_NAME",
          "TM_ID",
          "TM_NAME"
        ),
        new Configuration,
        Some(MyConstant.ZK_URL)
      )
      // 保存偏移量
      OffsetManager.saveOffset(topicName, groupId, offsetRanges)

    }
    ssc.start()
    ssc.awaitTermination()
  }

}
