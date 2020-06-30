package com.binbin.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.binbin.bean.UserInfo
import com.binbin.util.{MyConstant, MyDateUtils, MySparkUtils, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author libin
  * @create 2020-06-26 10:23 上午
  */
object UserInfoAPP {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf =
      new SparkConf().setAppName("UserInfoAPP").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topicName = "ODS_USER_INFO"
    val groupID = "user_info"
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] =
      MySparkUtils.getInputDStream(ssc, topicName, groupID)

    var offsetRanges: Array[OffsetRange] = null
    val dStream: DStream[ConsumerRecord[String, String]] = kafkaDS.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val userRDD: DStream[UserInfo] = dStream.transform { rdd =>
      val userRDD: RDD[UserInfo] = rdd.map { record =>
        val userJSON: JSONObject = JSON.parseObject(record.value())

        // 计算 年龄级别
        val birthday: String = userJSON.getString("birthday")
        val age: Int = MyDateUtils.getAge(birthday)
        val ageGroup: String = getAgeGroup(age)
        val gender: String = getGender(userJSON.getString("gender"))

        val userInfo: UserInfo = UserInfo(
          userJSON.getLong("id"),
          userJSON.getString("user_level"),
          age,
          ageGroup,
          gender
        )
        userInfo
      }
      userRDD
    }
    import org.apache.phoenix.spark._
    userRDD.foreachRDD { userRDD =>
      userRDD.saveToPhoenix(
        s"${MyConstant.HBASE_TABLE_PRE}_user_info",
        Seq(
          "ID",
          "USER_LEVEL",
          "USER_AGE",
          "USER_AGE_GROUP",
          "USER_GENDER"
        ),
        new Configuration,
        Some(MyConstant.ZK_URL)
      )
      OffsetManager.saveOffset(topicName, groupID, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def getGender(gender: String): String = {

    if (gender == null || gender.size == 0) {
      "保密"
    } else if (gender == "F") {
      "女"
    } else if (gender == "M") {
      "男"
    } else {
      "保密"
    }

  }

  def getAgeGroup(age: Int): String = {

    if (age < 20) {
      "20岁以下"
    } else if (age >= 20 && age < 30) {
      "20岁到30岁"
    } else {
      "30岁以上"
    }
  }
}
