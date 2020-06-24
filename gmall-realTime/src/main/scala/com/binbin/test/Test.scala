package com.binbin.test

/**
@author libin
@create 2020-06-23 5:43 下午 
*/object Test {
  def main(args: Array[String]): Unit = {
    val ints: List[Int] = List(1, 2, 4)
    val str: String = ints.mkString("','")
    println(str)
  }
}
