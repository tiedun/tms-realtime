package com.atguigu.tms.realtime.util;

import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;

public class CreateEnvUtil {

    /**
     * 初始化流处理环境，处理命令行参数，配置检查点
     *
     * @param args 命令行参数数组
     * @return 流处理环境
     */
    public static StreamExecutionEnvironment getStreamEnv(String[] args) {
        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
//                .createLocalEnvironmentWithWebUI(new Configuration());
                .getExecutionEnvironment();

//        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // TODO 2. 获取命令行参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // 获取 HDFS 用户名
        String hdfsUserName = parameterTool.get("hadoop-user-name", "atguigu");

        // TODO 3. 检查点
        // 3.1 启用检查点
        env.enableCheckpointing(60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        // 3.2 设置相邻两次检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000L);
        // 3.3 设置取消 Job 时检查点的清理模式
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 3.4 设置状态后端类型
        env.setStateBackend(new HashMapStateBackend());
        // 3.5 设置检查点存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/tms/ck/test");
        // 3.6 设置 HDFS 用户名
        System.setProperty("HADOOP_USER_NAME", hdfsUserName);

        return env;
    }

    /**
     * 初始化表处理环境，处理命令行参数，配置检查点
     *
     * @param args 命令行参数数组
     * @return 表处理环境
     */
    public static StreamTableEnvironment getTableEnv(String[] args) {
        StreamExecutionEnvironment env = getStreamEnv(args);
        return StreamTableEnvironment.create(env);
    }

    /**
     * 生成 Flink-CDC 的 MysqlSource 对象
     * @param option 选项，dim|dwd，对应不同的原始表列表
     * @param serverId  MySQL 从机的 serverId
     * @param args 命令行参数数组
     * @return MySqlSource 对象
     */
    public static MySqlSource<String> getJSONSchemaMysqlSource(String option, String serverId, String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mysqlHostname = parameterTool.get("mysql-hostname", "hadoop102");
        int mysqlPort = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        String mysqlUsername = parameterTool.get("mysql-username", "root");
        String mysqlPasswd = parameterTool.get("mysql-passwd", "000000");
        serverId = parameterTool.get("server-id", serverId);
        option = parameterTool.get("start-up-options", option);

        // 将 Decimal 类型数据的解析格式由 BASE64 更改为 NUMERIC，否则解析报错
        // 创建配置信息 Map 集合，将 Decimal 数据类型的解析格式配置 k-v 置于其中
        HashMap config = new HashMap<>();
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        // 将前述 Map 集合中的配置信息传递给 JSON 解析 Schema，该 Schema 将用于 MysqlSource 的初始化
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema =
                new JsonDebeziumDeserializationSchema(false, config);

        // 创建 MysqlSourceBuilder 对象
        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .username(mysqlUsername)
                .password(mysqlPasswd)
                .deserializer(jsonDebeziumDeserializationSchema);

        // 根据方法的 option 参数做不同的初始化操作，返回不同的 MysqlSource 对象
        switch (option) {
            // 配置表数据源
            case "config_dim":
                return builder
                        .databaseList("tms_config")
                        .tableList("tms_config.tms_config_dim")
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
//            // 离线维度数据源
//            case "offline_dim":
//                String[] dimTables = new String[]{"tms.user_info",
//                        "tms.user_address"};
//                return builder
//                        .databaseList("tms")
//                        .tableList(dimTables)
//                        .startupOptions(StartupOptions.initial())
//                        .serverId(serverId)
//                        .build();
            // 事实数据源（离线实时相同）
            case "dwd":
                String[] dwdTables = new String[]{"tms.order_info",
                        "tms.order_cargo",
                        "tms.transport_task",
                        "tms.order_org_bound"};
                return builder
                        .databaseList("tms")
                        .tableList(dwdTables)
                        .startupOptions(StartupOptions.latest())
                        .serverId(serverId)
                        .build();
            // 实时维度数据源
            case "realtime_dim":
                String[] realtimeDimTables = new String[]{"tms.user_info",
                        "tms.user_address",
                        "tms.base_complex",
                        "tms.base_dic",
                        "tms.base_region_info",
                        "tms.base_organ",
                        "tms.express_courier",
                        "tms.express_courier_complex",
                        "tms.employee_info",
                        "tms.line_base_shift",
                        "tms.line_base_info",
                        "tms.truck_driver",
                        "tms.truck_info",
                        "tms.truck_model",
                        "tms.truck_team"};
                return builder
                        .databaseList("tms")
                        .tableList(realtimeDimTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
        }
        Log.error("不支持的操作类型!");
        return null;
    }
}
