package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.TmsConfigDimBean;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.util.DimUtil;
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
import java.util.Map;
import java.util.Set;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 广播状态描述器
    private MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;

    // 配置表所在 MySQL DBMS 用户名
    private String user;
    // 配置表所在 MySQL DBMS 密码
    private String password;
    // 配置信息初始化对象
    private Map<String, TmsConfigDimBean> configMap = new HashMap<String, TmsConfigDimBean>();

    /**
     * 定义构造器用于获取命令行参数和广播状态描述器
     *
     * @param args               命令行参数
     * @param mapStateDescriptor 广播状态描述器
     */
    public MyBroadcastFunction(String[] args, MapStateDescriptor mapStateDescriptor) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        this.user = parameterTool.get("mysql-username", "root");
        this.password = parameterTool.get("mysql-passwd", "000000");
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 预加载配置信息
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

            // 数据脱敏
            switch (table) {
                // 员工表信息脱敏
                case "employee_info":
                    String empPassword = data.getString("password");
                    String empRealName = data.getString("real_name");
                    String idCard = data.getString("id_card");
                    String phone = data.getString("phone");

                    // 脱敏
                    empPassword = DigestUtils.md5Hex(empPassword);
                    empRealName = empRealName.charAt(0) +
                            empRealName.substring(1).replaceAll(".", "\\*");
                    idCard = idCard.matches("(^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{2}$)")
                            ? DigestUtils.md5Hex(idCard) : null;
                    phone = phone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(phone) : null;

                    data.put("password", empPassword);
                    data.put("real_name", empRealName);
                    data.put("id_card", idCard);
                    data.put("phone", phone);
                    break;
                // 快递员信息脱敏
                case "express_courier":
                    String workingPhone = data.getString("working_phone");
                    workingPhone = workingPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(workingPhone) : null;
                    data.put("working_phone", workingPhone);
                    break;
                // 卡车司机信息脱敏
                case "truck_driver":
                    String licenseNo = data.getString("license_no");
                    licenseNo = DigestUtils.md5Hex(licenseNo);
                    data.put("license_no", licenseNo);
                    break;
                // 卡车信息脱敏
                case "truck_info":
                    String truckNo = data.getString("truck_no");
                    String deviceGpsId = data.getString("device_gps_id");
                    String engineNo = data.getString("engine_no");

                    truckNo = DigestUtils.md5Hex(truckNo);
                    deviceGpsId = DigestUtils.md5Hex(deviceGpsId);
                    engineNo = DigestUtils.md5Hex(engineNo);

                    data.put("truck_no", truckNo);
                    data.put("device_gps_id", deviceGpsId);
                    data.put("engine_no", engineNo);
                    break;
                // 卡车型号信息脱敏
                case "truck_model":
                    String modelNo = data.getString("model_no");
                    modelNo = DigestUtils.md5Hex(modelNo);
                    data.put("model_no", modelNo);
                    break;
                // 用户地址信息脱敏
                case "user_address":
                    String addressPhone = data.getString("phone");
                    addressPhone = addressPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(addressPhone) : null;
                    data.put("phone", addressPhone);
                    break;
                // 用户信息脱敏
                case "user_info":
                    String passwd = data.getString("passwd");
                    String realName = data.getString("real_name");
                    String phoneNum = data.getString("phone_num");
                    String email = data.getString("email");

                    // 脱敏
                    passwd = DigestUtils.md5Hex(passwd);
                    realName = DigestUtils.md5Hex(realName);
                    phoneNum = phoneNum.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                            ? DigestUtils.md5Hex(phoneNum) : null;
                    email = email.matches("^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(.[a-zA-Z0-9_-]+)+$")
                            ? DigestUtils.md5Hex(email) : null;

                    data.put("passwd", passwd);
                    data.put("real_name", realName);
                    data.put("phone_num", phoneNum);
                    data.put("email", email);
                    break;
            }

            // 获取需要的字段列表
            String sinkColumns = tmsConfig.getSinkColumns();
            // 获取目标表名
            String sinkTable = tmsConfig.getSinkTable();

            // 过滤掉不需要的字段
            filterColumns(data, sinkColumns);

            // 将目标表名补充到 after 对象中
            data.put("sinkTable", sinkTable);

            // 获取主流数据操作类型
            String op = jsonObj.getString("op");

            // 清除缓存操作
            if (op.equals('u')) {
                // 无论修改了哪个字段，都删除主键拼接的 RedisKey
                String redisKey = "dim:" + sinkTable + ":id = '" + data.getString("id") + "'";
                DimUtil.deleteCached(redisKey);

                // 获取关联外键信息
                String foreignKeys = tmsConfig.getForeignKeys();

                // 若外键不为 null，则删除对应缓存
                if (foreignKeys != null) {
                    String[] foreignKeyArr = foreignKeys.split(",");
                    for (String foreignKey : foreignKeyArr) {
                        JSONObject before = jsonObj.getJSONObject("before");
                        String beforeVal = before.getString(foreignKey);
                        String afterVal = data.getString(foreignKey);
                        // 外键发生修改，删除历史值拼接获得的 key
                        if (!beforeVal.equals(afterVal)) {
                            redisKey = "dim:" + sinkTable + ":" +
                                    foreignKey + " = '" + beforeVal + "'";
                            // 外键未发生修改，直接删除对应 key
                        } else {
                            redisKey = "dim:" + sinkTable + ":" +
                                    foreignKey + " = '" + afterVal + "'";
                        }
                        DimUtil.deleteCached(redisKey);
                    }
                }
            }

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

            // 执行快照或插入数据时创建 Phoenix 维度表
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

    public static void main(String[] args) {
//        String x = "李依依";
//        DigestUtils.md5Hex(x);
//        String s = x.substring(0, 1) + x.substring(1, x.length()).replaceAll(".", "\\*");
//        System.out.println("s = " + s);
        String s = "13349792818";
        boolean matches = s.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$");
        System.out.println("matches = " + matches);
    }
}
