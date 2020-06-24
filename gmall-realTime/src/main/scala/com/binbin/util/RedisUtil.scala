package com.binbin.util

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author libin
  * @create 2020-06-17 4:57 下午
  */
object RedisUtil {

  var jedisPool: JedisPool = null

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient
    val keys: util.Set[String] = jedis.keys("*")
    println(keys.toString)
  }

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      //      println("开辟一个连接池")
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(10) //最大连接数
      jedisPoolConfig.setMaxIdle(4) //最大空闲
      jedisPoolConfig.setMinIdle(4) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    //    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    //   println("获得一个连接")
    jedisPool.getResource
  }
}
