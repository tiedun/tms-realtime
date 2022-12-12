package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwsTransDispatchDayBean {
    // 统计日期
    String curDate;

    // 订单ID
    @TransientSink
    String orderId;

    // 发单数
    Long dispatchOrderCountBase;

    // 时间戳
    Long ts;
}
