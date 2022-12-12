package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransOrgReceiveDayBean {

    // 统计日期
    String curDate;

    // 订单ID
    @TransientSink
    String orderId;

    // 小区ID
    @TransientSink
    String complexId;

    // 快递员ID
    @TransientSink
    String courierEmpId;

    // 转运站ID
    String orgId;

    // 转运站名称
    String orgName;

    // 地区ID
    String regionId;

    // 地区名称
    String regionName;

    // 揽收次数（一个订单算一次）
    Long receiveOrderCountBase;

    // 时间戳
    Long ts;
}
