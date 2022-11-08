package com.atguigu.tms.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.MyBroadcastFunction;
import com.atguigu.tms.realtime.app.func.MyPhoenixSink;
import com.atguigu.tms.realtime.bean.TmsConfigDim;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

        // TODO 3. 主流数据筛选及
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
        MySqlSource<String> mySqlSource = CreateEnvUtil.getJSONSchemaMysqlSource("config_dim", "6000", args);
        DataStreamSource<String> configSource =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        configSource.setParallelism(1);

        // TODO 5. 广播配置流
        MapStateDescriptor<String, TmsConfigDim> broadcastStateDescriptor =
                new MapStateDescriptor<>("tms-dim-config", String.class, TmsConfigDim.class);
        BroadcastStream<String> broadcastStream = configSource.broadcast(broadcastStateDescriptor);
        BroadcastConnectedStream<String, String> connectedStream =
                filteredStream.connect(broadcastStream);

        // TODO 6. 处理连接流
        SingleOutputStreamOperator<JSONObject> processedStream = connectedStream.process(
                new MyBroadcastFunction(args, broadcastStateDescriptor)
        );

        // TODO 7. 将数据写出到 Phoenix
        processedStream.addSink(new MyPhoenixSink());

        env.execute();
    }
}
