package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class DwsTransOrgTruckModelTransFinishDayBean {
    // 统计日期
    String curDate;

    // 机构ID
    String orgId;

    // 机构名称
    String orgName;

    // 卡车ID
    @TransientSink
    String truckId;

    // 卡车型号ID
    String truckModelId;

    // 卡车型号名称
    String truckModelName;

    // 用于关联城市信息的一级机构ID
    @TransientSink
    String joinOrgId;

    // 城市ID
    String cityId;

    // 城市名称
    String cityName;

    // 转运完成次数
    Long transFinishCountBase;

    // 转运完成里程
    BigDecimal transFinishDistanceBase;

    // 转运完成历经时长
    Long transFinishDurTimeBase;

    // 时间戳
    Long ts;
}
