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
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

        // TODO 2. 获取命令行参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // 2.1 获取检查点触发间隔
        long checkpointInterval = Long.parseLong(
                parameterTool.get("checkpoint-interval", 60 * 1000 + ""));
        // 2.2 获取两次检查点的最小时间间隔（若同时执行的最大检查点数量为 1 则上一个检查点执行完毕后必须等待最小间隔指定的时间，下一个检查点才可以触发）
        long minPause = Long.parseLong(
                parameterTool.get("min-pause", 30 * 1000 + ""));
        // 2.3 获取外部检查点模式
        String externalizedMode = parameterTool.get("externalized-mode", "retain");
        // 2.4 获取状态后端类型
        String stateBackendType = parameterTool.get("state-backend-type", "hashmap");
        // 2.5 获取检查点 URL
        String checkpointUrl = parameterTool.get("checkpoint-url", "hdfs://mycluster/tms/ck");
        // 2.6 获取 HDFS 用户名
        String hdfsUserName = parameterTool.get("hadoop-user-name", "atguigu");

        // TODO 3. 检查点
        // 3.1 启用检查点
        env.enableCheckpointing(checkpointInterval);
        // 3.2 设置相邻两次检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);

        // 3.3 设置取消 Job 时检查点的清理模式
        CheckpointConfig.ExternalizedCheckpointCleanup cleanUpMode = null;
        switch (externalizedMode) {
            case "delete":
                // 取消 Job 后清除对应检查点
                cleanUpMode = CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION;
                break;
            case "disable":
                // 禁用外部检查点
                cleanUpMode = CheckpointConfig.ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS;
                break;
            case "retain":
                // 取消 Job 后保留外部检查点
                cleanUpMode = CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
        }
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(cleanUpMode);

        // 3.4 设置状态后端类型
        AbstractStateBackend abstractStateBackend = null;
        switch (stateBackendType) {
            case "hashmap":
                abstractStateBackend = new HashMapStateBackend();
                break;
            case "rocksdb":
                abstractStateBackend = new EmbeddedRocksDBStateBackend();
        }
        env.setStateBackend(abstractStateBackend);

        // 3.5 设置检查点存储路径
        env.getCheckpointConfig().setCheckpointStorage(checkpointUrl);
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

    public static MySqlSource<String> getJSONSchemaMysqlSource(String option, String serverId, String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mysqlHostname = parameterTool.get("mysql-hostname", "hadoop102");
        int mysqlPort = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        String mysqlUsername = parameterTool.get("mysql-username", "root");
        String mysqlPasswd = parameterTool.get("mysql-passwd", "000000");
        serverId = parameterTool.get("server-id", serverId);
        option = parameterTool.get("start-up-options", option);

        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .username(mysqlUsername)
                .password(mysqlPasswd)
                .deserializer(new JsonDebeziumDeserializationSchema());
        switch (option) {
            case "config_dim":
                return builder
                        .databaseList("tms_config")
                        .tableList("tms_config.tms_config_dim")
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
            case "config_dwd":
                return builder
                        .databaseList("tms_config")
                        .tableList("tms_config.tms_config_dwd")
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
            case "dim":
               String[] dimTables = new String[] {"tms.base_complex",
                        "tms.base_organ",
                        "tms.base_region_info",
                        "tms.employee_info",
                        "tms.express_courier",
                        "tms.line_base_info",
                        "tms.line_base_shift",
                        "tms.truck_driver",
                        "tms.truck_info",
                        "tms.truck_model",
                        "tms.truck_team",
                        "tms.user_address",
                        "tms.user_info",
                        "tms.base_dic"};
                return builder
                        .databaseList("tms")
                        .tableList(dimTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
            case "dwd":
                String[] dwdTables = new String[] {"tms.base_complex",
                        "tms.express_task_collect",
                        "tms.express_task_delivery",
                        "tms.order_cargo",
                        "tms.order_info",
                        "tms.order_org_bound",
                        "tms.order_trace_log",
                        "tms.transport_task",
                        "tms.transport_task_detail",
                        "tms.transport_task_process"};
                return builder
                        .databaseList("tms")
                        .tableList(dwdTables)
                        .startupOptions(StartupOptions.latest())
                        .serverId(serverId)
                        .build();
        }
        Log.error("不支持的操作类型!");
        return null;
    }
}
