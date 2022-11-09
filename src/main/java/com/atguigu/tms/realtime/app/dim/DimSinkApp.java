package com.atguigu.tms.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.MyBroadcastFunction;
import com.atguigu.tms.realtime.app.func.MyPhoenixSink;
import com.atguigu.tms.realtime.bean.TmsConfigDimBean;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 获取流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // TODO 2. 读取主流数据
        String topic = "tms_ods";
        String groupId = "tms_dim_sink_app";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 主流数据过滤及结构转换
        SingleOutputStreamOperator<JSONObject> flatMappedStream = source.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        String table = jsonObj.getJSONObject("source").getString("table");
                        jsonObj.put("table", table);
                        jsonObj.remove("before");
                        jsonObj.remove("source");
                        jsonObj.remove("transaction");
                        out.collect(jsonObj);
                    }
                }
        );

        // TODO 4. 读取配置流
        MySqlSource<String> mySqlSource = CreateEnvUtil.getJSONSchemaMysqlSource("config_dim", "6000", args);
        DataStreamSource<String> configSource =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source")
                        .setParallelism(1);

        // TODO 5. 广播配置流
        MapStateDescriptor<String, TmsConfigDimBean> broadcastStateDescriptor =
                new MapStateDescriptor<>("tms-dim-config", String.class, TmsConfigDimBean.class);
        BroadcastStream<String> broadcastStream = configSource.broadcast(broadcastStateDescriptor);
        BroadcastConnectedStream<JSONObject, String> connectedStream =
                flatMappedStream.connect(broadcastStream);

        // TODO 6. 处理连接流
        SingleOutputStreamOperator<JSONObject> processedStream = connectedStream.process(
                new MyBroadcastFunction(args, broadcastStateDescriptor)
        );

        // TODO 7. 将数据写出到 Phoenix
        processedStream.addSink(new MyPhoenixSink());

        env.execute();
    }
}
