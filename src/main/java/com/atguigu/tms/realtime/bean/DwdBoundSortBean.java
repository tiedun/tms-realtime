package com.atguigu.tms.realtime.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DwdBoundSortBean {
    // 编号（主键）
    String id;

    // 运单编号
    String orderId;

    // 机构id
    String orgId;

    // 分拣时间
    String sortTime;

    // 分拣人员id
    String sorterEmpId;

    // 时间戳
    Long ts;
}
