package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.Set;

public class MyPhoenixSink implements SinkFunction<JSONObject> {
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段和 type 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");
        jsonObj.remove("type");

        // 获取字段名
        Set<String> columns = jsonObj.keySet();
        // 获取字段对应的值
        Collection<Object> values = jsonObj.values();
        // 拼接字段名
        String columnStr = StringUtils.join(columns, ",");
        // 拼接字段值
        String valueStr = StringUtils.join(values, "','");
        // 拼接插入语句
        String sql = "upsert into " + TmsConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";

        PhoenixUtil.executeSQL(sql);
    }
}
