package com.binbin.util

import java.sql.{
  Connection,
  DriverManager,
  ResultSet,
  ResultSetMetaData,
  Statement,
  Array => _
}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * @author libin
  * @create 2020-06-30 8:22 下午
  */
object MysqlUtil {

  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList(
      "SELECT partition_id , topic_offset FROM offset"
    )
    list.foreach(println)
  }

  def queryList(sql: String) = {
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
    val conn: Connection = DriverManager.getConnection(
      "jdbc:mysql://hadoop102:3306/gmall_rs?characterEncoding=utf-8&useSSL=false",
      "root",
      "000000"
    )
    val statement: Statement = conn.createStatement
    val resultSet: ResultSet = statement.executeQuery(sql)
    val metaData: ResultSetMetaData = resultSet.getMetaData
    while (resultSet.next()) {
      val rowData = new JSONObject()
      for (i <- 1 to metaData.getColumnCount) {
        rowData.put(metaData.getColumnName(i), resultSet.getObject(i))
      }
      resultList += rowData
    }
    statement.close()
    conn.close()
    resultList.toList
  }

}
