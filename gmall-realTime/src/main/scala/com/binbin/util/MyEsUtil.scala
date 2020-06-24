package com.binbin.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
  * @author libin
  * @create 2020-06-17 4:41 下午
  */
object MyEsUtil {
  var factory: JestClientFactory = null;

  def getClient: JestClient = {
    if (factory == null) {
      build();
    }
    factory.getObject
  }

  def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder("http://hadoop102:9200")
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000)
        .readTimeout(10000)
        .build()
    )
  }

  def addDoc() = {
    val jest: JestClient = getClient
    val index = new Index.Builder(Movie0105("0104", "龙岭迷窟", "鬼吹灯"))
      .index("movie0105_test_20200619")
      .`type`("_doc")
      .id("0104")
      .build()

    val message: String = jest.execute(index).getErrorMessage
    if (message != null) {
      println(message)
    }
    jest.close()
  }

  def bulkDoc(dataList: List[Any], indexName: String) = {
    if (dataList != null && dataList.size > 0) {
      val jestClient: JestClient = getClient
      val bulkBuilder: Bulk.Builder = new Bulk.Builder
      for (data <- dataList) {
        val index: Index =
          new Index.Builder(data).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jestClient.execute(bulk)

      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println(s"保存到es:${items.size}条数据")
      jestClient.close()
    }
  }

}

case class Movie0105(id: String, movie_name: String, name: String);
