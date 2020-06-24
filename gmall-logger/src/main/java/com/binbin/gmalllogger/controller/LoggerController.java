package com.binbin.gmalllogger.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;

/**
 * @author libin
 * @create 2020-06-13 5:53 下午
 */
@Slf4j
@RestController
public class LoggerController {

    private String GMALL_STARTUP_0105 = "GMALL_STARTUP_0105";
    private String GMALL_EVENT_0105 = "GMALL_EVENT_0105";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("applog")
    public String applog(@RequestBody String logString) {

         log.info(logString);
        // 分流
        JSONObject jsonObject = JSON.parseObject(logString);
        if (StringUtils.isNotBlank(jsonObject.getString("start"))) {
            kafkaTemplate.send(GMALL_STARTUP_0105, logString);
        } else {
            kafkaTemplate.send(GMALL_EVENT_0105, logString);
        }
        return "success";
    }

}
