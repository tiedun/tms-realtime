package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransBoundFinishDayBean {
    // 统计日期
    String curDate;

    // 订单ID
    @TransientSink
    String orderId;

    // 转运完成次数
    Long boundFinishOrderCountBase;

    // 时间戳
    Long ts;
}
