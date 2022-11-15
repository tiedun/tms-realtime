package com.atguigu.tms.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class DwsTradeOrgOrderDayBean {

    // 日期
    String curDate;

    // 机构ID
    String orgId;

    // 机构名称
    String orgName;

    // 城市编码
    String cityCode;

    // 城市名称
    String cityName;

    // 快递员ID
    @TransientSink
    String courierEmpId;

    // 下单金额
    BigDecimal order_amount_base;

    // 下单次数
    Long order_count_base;

    // 时间戳
    Long ts;
}
