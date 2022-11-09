package com.atguigu.tms.realtime.bean;

import lombok.Data;

@Data
public class TmsConfigDimBean {
    // 数据源表表名
    String sourceTable;

    // 目标表表名
    String sinkTable;

    // 需要的字段列表
    String sinkColumns;

    // 主键字段
    String primaryKey;

    // 建表扩展字段（指定盐值）
    String sinkExtend;
}
