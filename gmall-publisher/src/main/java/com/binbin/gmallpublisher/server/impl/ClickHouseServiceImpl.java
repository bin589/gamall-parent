package com.binbin.gmallpublisher.server.impl;

import com.binbin.gmallpublisher.dao.OrderWideMapper;
import com.binbin.gmallpublisher.server.ClickHouseService;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

  @Autowired
  OrderWideMapper orderWideMapper;

  @Override
  public BigDecimal getOrderAmount(String date) {
    return orderWideMapper.selectOrderAmount(date);
  }

  @Override
  public Map getOrderAmountHour(String date) {
    //进行转换
    List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
    Map<String, BigDecimal> hourMap = new HashMap<>();
    for (Map map : mapList) {
      hourMap.put(String.valueOf(map.get("hr")), (BigDecimal) map.get("order_amount"));
    }
    return hourMap;
  }
}
