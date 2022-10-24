package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * description:
 * Created by 铁盾 on 2022/10/24
 */
public class MyBroadcastFunction extends BroadcastProcessFunction<String, String, String> {

    private MapStateDescriptor<String, String> mapStateDescriptor;
    private String username;
    private String password;
    private Map<String, String> configMap = new HashMap<String, String>();

    public MyBroadcastFunction(String username, String password, MapStateDescriptor mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

//        DriverManager.registerDriver(new Driver());
//        System.setProperty("jdbc.drivers", "com.mysql.jdbc.Driver");
        // 1. 注册驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 2. 获取连接对象
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/tms_config?useSSL=false&useUnicode=true" +
                "&username=" + username + "&password=" + password +
                "&charset=utf8&TimeZone=Asia/Shanghai");
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
            configMap.put(primaryKey, jsonObj.toJSONString());
        }

        // 6. 释放资源
        rs.close();
        conn.close();
    }

    @Override
    public void processElement(String jsonStr, ReadOnlyContext context, Collector<String> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String table = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, String> broadcastState =
                context.getBroadcastState(mapStateDescriptor);
        String tmsConfig = broadcastState.get(table);
        if (tmsConfig != null) {

        }
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<String> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        JSONObject after = jsonObj.getJSONObject("after");
        String sourceTable = after.getString("source_table");
        BroadcastState<String, String> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(sourceTable, after.toJSONString());
    }
}
