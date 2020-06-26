package com.binbin.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author libin
  * @create 2020-06-26 10:39 上午
  */
object MyDateUtils {

  val patternYMD = "yyyy-MM-dd"

  def getAge(startDateStr: String) = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(patternYMD)
    val nowStr = dateFormat.format(now)
    val age: Long = getDiffYear(startDateStr, nowStr)
    age.toInt
  }

  def getDiffYear(startDateStr: String, endDateStr: String): Long = {
    val between: Long = getDiffTime(startDateStr, endDateStr)
    val year: Long = between / 1000 / 3600 / 24 / 365
    year
  }

  def getDiffTime(startDateStr: String, endDateStr: String): Long = {
    val startDate = new SimpleDateFormat(patternYMD).parse(startDateStr)
    val endDate = new SimpleDateFormat(patternYMD).parse(endDateStr)
    val between = endDate.getTime - startDate.getTime
    between
  }
}
