package com.binbin.gmallpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
public class GmallPublisherApplication {

  public static void main(String[] args) {

    SpringApplication.run(GmallPublisherApplication.class, args);
  }

}
