package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;

@Data
@Builder
public class DwsTradeOrgOrderDayBean {

    // 日期
    String curDate;

    // 机构ID
    String orgId;

    // 机构名称
    String orgName;

    // 城市ID
    String cityId;

    // 城市名称
    String cityName;

    // 订单ID
    @TransientSink
    String orderId;

    // 小区ID
    @TransientSink
    String complexId;

    // 快递员ID
    @TransientSink
    String courierEmpId;

    // 下单金额
    BigDecimal orderAmountBase;

    // 下单次数
    Long orderCountBase;

    // 时间戳
    Long ts;
}
