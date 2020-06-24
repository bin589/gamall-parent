package com.binbin.gmalllogger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author libin
 * @create 2020-06-13 5:50 下午
 */
@RestController
public class DemoController {

    @RequestMapping("helloWord")
    public String helloWord(){
        return "helloWord";
    }
}
