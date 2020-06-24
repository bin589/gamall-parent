package com.binbin.gmallpublisher.server;

import java.util.Map;

/**
 * @author libin
 * @create 2020-06-20 4:40 下午
 */
public interface EsService {

    /**
     * 获取总数
     * 
     * @param date
     * @return
     */
    Long getDauTotal(String date);

    /**
     * 获取每小时数量
     * 
     * @param date
     * @return
     */
    Map<String, Long> getDauHour(String date);

}
