package com.binbin.util

import java.sql._

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * @author libin
  * @create 2020-06-23 11:11 上午
  */
object PhoenixUtil {

  def queryList(sql: String): List[JSONObject] = {
    println(s"执行${sql}")
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val conn: Connection = DriverManager.getConnection(
      "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
    )

    val stat: Statement = conn.createStatement()
    val rs: ResultSet = stat.executeQuery(sql)
    val rsmd: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      val rowData: JSONObject = new JSONObject()
      for (i <- 1 to rsmd.getColumnCount) {
        rowData.put(rsmd.getColumnName(i), rs.getObject(i))
      }
      resultList += rowData
    }
    stat.close()
    conn.close()
    resultList.toList

  }

}
