package com.binbin.bean

/**
@author libin
@create 2020-06-25 9:57 上午
  */
case class OrderDetail(id: Long,
                       order_id: Long,
                       sku_id: Long,
                       order_price: Double,
                       sku_num: Long,
                       sku_name: String,
                       create_time: String,
                       // 下面的是维度表数据
                       var spu_id: Long,
                       var spu_name: String,
                       var tm_id: Long,
                       var tm_name: String,
                       var category3_id: Long,
                       var category3_name: String){
  
  
}
