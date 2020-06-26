package com.binbin.bean

/**
@author libin
@create 2020-06-26 10:16 上午
  */
case class UserInfo(id: Long,
                    name: String,
                    phone_num: String,
                    email: String,
                    user_level: String,
                    var user_age: Int,
                    var user_age_group: Int,
                    var user_gender: String)
