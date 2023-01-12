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

    // 转运站ID
    String orgId;

    // 转运站名称
    String orgName;

    // 地区ID
    @TransientSink
    String districtId;

    // 城市ID
    String cityId;

    // 城市名称
    String cityName;

    // 省份ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 揽收次数（一个订单算一次）
    Long receiveOrderCountBase;

    // 时间戳
    Long ts;
}
