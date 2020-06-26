package com.binbin.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.{BaseCategory3, BaseTrademark, SkuInfo}
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
  * 保存sku到hbase维度表
  *
  *@author libin
  *@create 2020-06-25 10:14 上午
  */
object SkuInfoAPP {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setMaster("local[4]").setAppName("SkuInfoAPP")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicName = "ODS_SKU_INFO"
    val groupId = "sku_info"
    val inputDStream: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, topicName, groupId)

    var offsetRanges: Array[OffsetRange] = null
    val dStream: DStream[ConsumerRecord[String, String]] =
      inputDStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    val skuInfoDS: DStream[SkuInfo] = dStream.map { ds =>
      val objStr: String = ds.value()
      val skuInfo: SkuInfo = JSON.parseObject(objStr, classOf[SkuInfo])
      skuInfo

    }

    // 使用广播变量查数据
    // spu_name
    val skuInfoWithSpuDS: DStream[SkuInfo] = skuInfoDS.transform { rdd =>
      // driver查询分类
      val tmSql =
        s"select id, spu_name from ${MyConstant.HBASE_TABLE_PRE}_spu_info";
      val jSONObjects: List[JSONObject] = PhoenixUtil.queryList(tmSql)

      val spuMap: Map[String, String] = jSONObjects.map { jsonObj =>
        (jsonObj.getString("ID"), jsonObj.getString("SPU_NAME"))
      }.toMap

      val tmBro: Broadcast[Map[String, String]] =
        ssc.sparkContext.broadcast(spuMap)

      val skuRdd: RDD[SkuInfo] = rdd.map { skuInfo =>
        val tmMap: Map[String, String] = tmBro.value
        val spuName: String = tmMap.getOrElse(skuInfo.tm_id, null)
        if (spuName != null) {
          skuInfo.spu_name = spuName
        }
        skuInfo
      }

      skuRdd
    }

    // tm_name
    val skuInfoWithTmDS: DStream[SkuInfo] = skuInfoWithSpuDS.transform { rdd =>
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

      val skuRdd: RDD[SkuInfo] = rdd.map { skuInfo =>
        val tmMap: Map[String, BaseTrademark] = tmBro.value
        val trademark: BaseTrademark = tmMap.getOrElse(skuInfo.tm_id, null)
        if (trademark != null) {
          skuInfo.tm_name = trademark.tm_name
        }
        skuInfo
      }

      skuRdd
    }

    //  category3_name
    val skuInfoWithTmDAndCate: DStream[SkuInfo] = skuInfoWithTmDS.transform {
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

        val skuRdd: RDD[SkuInfo] = rdd.map { skuInfo =>
          val cateMap: Map[String, BaseCategory3] = cateBC.value
          val category: BaseCategory3 =
            cateMap.getOrElse(skuInfo.category3_id, null)
          if (category != null) {
            skuInfo.category3_name = category.name
          }
          skuInfo
        }
        skuRdd

    }

    // 保存到HBASE
    import org.apache.phoenix.spark._
    skuInfoWithTmDAndCate.foreachRDD { rdd =>
      rdd.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_sku_info",
        Seq(
          "ID",
          "SKU_NAME",
          "SPU_ID",
          "SPU_NAME",
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
