package com.binbin.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.binbin.gmallpublisher.server.ClickHouseService;
import com.binbin.gmallpublisher.server.EsService;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author libin
 * @create 2020-06-20 5:25 下午
 */
@RestController
public class PublisherController {

  @Autowired
  private EsService esService;
  @Autowired
  ClickHouseService clickHouseService;


  @RequestMapping(value = "realtime-total", method = RequestMethod.GET)
  public String realtimeTotal(@RequestParam("date") String date) {
    // [{"id":"dau","name":"新增日活","value":1200},{"id":"new_mid","name":"新增设备","value":233} ]
    List<Map<String, String>> dataList = new ArrayList<>();

    Map<String, String> exerciseMap = new HashMap<>();
    exerciseMap.put("id", "dau");
    exerciseMap.put("name", "新增日活");
//    Long total = esService.getDauTotal(date);
    exerciseMap.put("value", "10");
    dataList.add(exerciseMap);

    Map<String, String> deviceMap = new HashMap<>();
    deviceMap.put("id", "dau");
    deviceMap.put("name", "新增日活");
    BigDecimal orderAmount = clickHouseService.getOrderAmount(date);
    deviceMap.put("value", orderAmount.toString());
    dataList.add(deviceMap);

    return JSON.toJSONString(dataList);
  }

}
