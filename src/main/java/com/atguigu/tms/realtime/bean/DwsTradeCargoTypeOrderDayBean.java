package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class DwsTradeCargoTypeOrderDayBean {
    // 当前日期
    String curDate;

    // 货物类型ID
    String cargoType;

    // 货物类型名称
    String cargoTypeName;

    // 下单金额
    BigDecimal orderAmountBase;

    // 运单ID
    @TransientSink
    String orderId;

    // 下单次数
    Long orderCountBase;

    // 时间戳
    Long ts;
}
