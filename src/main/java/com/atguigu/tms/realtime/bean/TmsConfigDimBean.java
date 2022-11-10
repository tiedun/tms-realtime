package com.atguigu.tms.realtime.bean;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Data
public class TmsConfigDimBean {
    // 数据源表表名
    String SourceTable;

    // 目标表表名
    String SinkTable;

    // 需要的字段列表
    String SinkColumns;

    // 主键字段
    String PrimaryKey;

    // 建表扩展字段（指定盐值）
    String SinkExtend;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("{\n" +
                        "\t\"source_table\": \"source_test\",\n" +
                        "\t\"sink_columns\": \"id, name\",\n" +
                        "\t\"primary_key\": \"id\",\n" +
                        "\t\"sink_extend\": \"salt_buckets=4\",\n" +
                        "\t\"sink_table\": \"sink_test\"\n" +
                        "}")
                .map(r -> JSON.parseObject(r, TmsConfigDimBean.class))
                        .print();

        TmsConfigDimBean tmsConfigDimBean = new TmsConfigDimBean();
//        TmsConfigDimBean f2 = new TmsConfigDimBean("1", "f2", "", "", "");

        env.execute();
    }
}
