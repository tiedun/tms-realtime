package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.TmsConfigDimBean;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.util.PhoenixUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;
    private String user;
    private String password;
    private Map<String, TmsConfigDimBean> configMap = new HashMap<String, TmsConfigDimBean>();

    public MyBroadcastFunction(String[] args, MapStateDescriptor mapStateDescriptor) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        this.user = parameterTool.get("mysql-username", "root");
        this.password = parameterTool.get("mysql-passwd", "000000");
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
                "&user=" + user + "&password=" + password +
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
            String key = null;
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String value = rs.getString(i);
                jsonObj.put(columnName, value);
                if (columnName.equals("source_table")) {
                    key = value;
                }
            }
            configMap.put(key, jsonObj.toJavaObject(TmsConfigDimBean.class));
        }

        // 6. 释放资源
        rs.close();
        conn.close();
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext context, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, TmsConfigDimBean> broadcastState =
                context.getBroadcastState(mapStateDescriptor);
        TmsConfigDimBean tmsConfig = broadcastState.get(table);
        if (tmsConfig == null) {
            tmsConfig = configMap.get(table);
        }

        if (tmsConfig != null) {
            JSONObject data = jsonObj.getJSONObject("after");

            // 对用户表数据进行脱敏
            if (table.equals("user_info")) {
                String passwd = data.getString("passwd");
                String realName = data.getString("real_name");
                String phoneNum = data.getString("phone_num");
                String email = data.getString("email");

                // 脱敏
                passwd = DigestUtils.md5Hex(passwd);
                realName = DigestUtils.md5Hex(realName);
                phoneNum = DigestUtils.md5Hex(phoneNum);
                email = DigestUtils.md5Hex(email);

                data.put("passwd", passwd);
                data.put("real_name", realName);
                data.put("phone_num", phoneNum);
                data.put("email", email);
            }

            // 获取需要的字段列表
            String sinkColumns = tmsConfig.getSinkColumns();
            // 获取目标表名
            String sinkTable = tmsConfig.getSinkTable();
            // 获取主流数据操作类型
//            String op = jsonObj.getString("op");

            // 过滤掉不需要的字段
            filterColumns(data, sinkColumns);

            // 将目标表名补充到 after 对象中
            data.put("sinkTable", sinkTable);
            // 将操作类型补充到 after 对象中
//            data.put("op", op);

            // 将主流数据发送到下游
            out.collect(data);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        // 删除下游不需要的字段
        entries.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String op = jsonObj.getString("op");
        BroadcastState<String, TmsConfigDimBean> broadcastState = context.getBroadcastState(mapStateDescriptor);

        if ("d".equals(op)) {
            String sourceTable = jsonObj.getJSONObject("before").getString("source_table");
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            TmsConfigDimBean config = jsonObj.getObject("after", TmsConfigDimBean.class);
            String sourceTable = config.getSourceTable();
            // 更新广播状态
            broadcastState.put(sourceTable, config);
            // 更新预加载对象
            configMap.put(sourceTable, config);

            // 快照或插入数据时创建 Phoenix 维度表
            if ("c".equals(op) || "r".equals(op)) {
                String sinkTable = config.getSinkTable();
                String sinkColumns = config.getSinkColumns();
                String sinkPk = config.getPrimaryKey();
                String sinkExtend = config.getSinkExtend();
                checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
            }
        }
    }

    /**
     * Phoenix 建表函数
     *
     * @param sinkTable   目标表名  eg. test
     * @param sinkColumns 目标表字段  eg. id,name,sex
     * @param sinkPk      目标表主键  eg. id
     * @param sinkExtend  目标表建表扩展字段  eg. ""
     *                    eg. create table if not exists mydb.test(id varchar primary key, name varchar, sex varchar)...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + TmsConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        // 为主键及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();

        PhoenixUtil.executeSQL(createStatement);
    }

}
