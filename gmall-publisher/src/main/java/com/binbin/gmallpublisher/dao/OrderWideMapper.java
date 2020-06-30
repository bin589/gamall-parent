package com.binbin.gmallpublisher.dao;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author libin
 * @create 2020-06-30 7:52 下午
 */
@Mapper
public interface OrderWideMapper {

  //查询当日总额
  BigDecimal selectOrderAmount(String date);

  //查询当日分时交易额
  List<Map> selectOrderAmountHour(String date);
}
