package com.atguigu.tms.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.MyBroadcastFunction;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 获取流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // TODO 2. 读取主流数据
        String topic = "tms_ods";
        String groupId = "tms_dim_sink_app";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 主流 ETL
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        if (jsonStr != null) {
                            try {
                                JSONObject jsonObj = JSON.parseObject(jsonStr);
                                String type = jsonObj.getString("type");
                                return !type.equals("bootstrap-start")
                                        && !type.equals("bootstrap-complete");
                            } catch (Exception e) {
                                e.printStackTrace();
                                return false;
                            }
                        }
                        return false;
                    }
                }
        );

        // TODO 4. 读取配置流
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mysqlHostname = parameterTool.get("mysql-hostname", "hadoop102");
        int mysqlPort = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        String mysqlUsername = parameterTool.get("mysql-username", "root");
        String mysqlPasswd = parameterTool.get("mysql-passwd", "000000");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .username(mysqlUsername)
                .password(mysqlPasswd)
                .databaseList("tms_config")
                .tableList("tms_config.tms_config_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> configSource =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        configSource.setParallelism(1);

        // TODO 5. 广播配置流
        MapStateDescriptor<String, String> broadcastStateDescriptor =
                new MapStateDescriptor<>("tms-dim-config", String.class, String.class);
        BroadcastStream<String> broadcastStream = configSource.broadcast(broadcastStateDescriptor);
        BroadcastConnectedStream<String, String> connectedStream =
                filteredStream.connect(broadcastStream);

        // TODO 6. 处理连接流
        connectedStream.process(
                new MyBroadcastFunction(mysqlUsername, mysqlPasswd, broadcastStateDescriptor)
        );



        env.execute();
    }
}
