package com.binbin.gmallpublisher.server;

import java.math.BigDecimal;
import java.util.Map;

public interface ClickHouseService {

  BigDecimal getOrderAmount(String date);

  Map getOrderAmountHour(String date);


}
