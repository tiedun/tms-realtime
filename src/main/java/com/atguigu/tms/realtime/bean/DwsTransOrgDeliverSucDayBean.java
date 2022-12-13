package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransOrgDeliverSucDayBean {
    // 统计日期
    String curDate;

    // 小区 ID
    @TransientSink
    String complexId;

    // 快递员 ID
    @TransientSink
    String courierEmpId;

    // 机构 ID
    String orgId;

    // 机构名称
    String orgName;

    // 地区 ID
    String regionId;

    // 地区名称
    String regionName;

    // 城市 ID
    String cityId;

    // 城市名称
    String cityName;

    // 运单 ID
    @TransientSink
    String orderId;

    // 派送成功次数
    Long deliverSucCountBase;

    // 时间戳
    Long ts;
}
