package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.TmsConfigDim;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * Created by 铁盾 on 2022/10/24
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<String, String, JSONObject> {

    private MapStateDescriptor<String, TmsConfigDim> mapStateDescriptor;
    private String username;
    private String password;
    private Map<String, TmsConfigDim> configMap = new HashMap<String, TmsConfigDim>();

    public MyBroadcastFunction(String[] args, MapStateDescriptor mapStateDescriptor) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        this.username = parameterTool.get("mysql-username", "root");
        this.password  = parameterTool.get("mysql-passwd", "000000");
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

//        DriverManager.registerDriver(new Driver());
//        System.setProperty("jdbc.drivers", "com.mysql.jdbc.Driver");
        // 1. 注册驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 2. 获取连接对象
        String url = "jdbc:mysql://hadoop102:3306/tms_config?useSSL=false&useUnicode=true" +
                "&user=" + username + "&password=" + password +
                "&charset=utf8&TimeZone=Asia/Shanghai";
        Connection conn = DriverManager.getConnection(url);
        // 3. 获取数据库编译对象
        PreparedStatement preparedStatement =
                conn.prepareStatement("select * from tms_config.tms_config_dim");
        // 4. 执行 SQL
        ResultSet rs = preparedStatement.executeQuery();
        // 5. 解析结果集
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            String primaryKey = null;
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String value = rs.getString(i);
                jsonObj.put(columnName, value);
                if (columnName.equals("source_table")) {
                    primaryKey = value;
                }
            }
            configMap.put(primaryKey, jsonObj.toJavaObject(TmsConfigDim.class));
        }

        // 6. 释放资源
        rs.close();
        conn.close();
    }

    @Override
    public void processElement(String jsonStr, ReadOnlyContext context, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String table = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, TmsConfigDim> broadcastState =
                context.getBroadcastState(mapStateDescriptor);
        TmsConfigDim tmsConfig = broadcastState.get(table);
        if (tmsConfig == null) {
            tmsConfig = configMap.get(table);
        }

        if (tmsConfig != null) {
            JSONObject data = jsonObj.getJSONObject("data");

            // 获取需要的字段名称
            String sinkColumns = tmsConfig.getSinkColumns();
            // 获取目标表名
            String sinkTable = tmsConfig.getSinkTable();
            // 获取主流数据操作类型
            String type = jsonObj.getString("type");

            // 筛选需要的字段
            filterColumns(data, sinkColumns);

            // 将目标表名补充到 data 对象中
            data.put("sinkTable", sinkTable);
            // 将操作类型补充到 data 对象中
            data.put("type", type);

            // 将主流数据发送到下游
            out.collect(data);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(r -> sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String op = jsonObj.getString("op");
        BroadcastState<String, TmsConfigDim> broadcastState = context.getBroadcastState(mapStateDescriptor);

        if("d".equals(op)) {
            String sourceTable = jsonObj.getJSONObject("before").getString("source_table");
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            TmsConfigDim after = jsonObj.getObject("after", TmsConfigDim.class);
            String sourceTable = after.getSourceTable();
            // 更新广播状态
            broadcastState.put(sourceTable, after);
            // 更新预加载对象
            configMap.put(sourceTable, after);
        }
    }
}
