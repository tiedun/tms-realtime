package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * description:
 * Created by 铁盾 on 2022/11/9
 */
public class DwdOrderRelevantApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // TODO 2. 读取事实数据
        String topic = "ods_tms";
        String groupId = "dwd_order_relevant_app";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 筛选数据形成订单流和明细流
        // 3.1 筛选 order_info 表数据
        SingleOutputStreamOperator<String> orderInfoStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        return table.equals("order_info");
                    }
                }
        );

        // 3.2 筛选 order_cargo 表数据
        SingleOutputStreamOperator<String> orderCargoStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        return table.equals("order_cargo");
                    }
                }
        );

        // TODO 4. 转换数据结构
        // 4.1 order_info 表数据
        SingleOutputStreamOperator<JSONObject> orderInfoMappedStream = orderInfoStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        String table = jsonObj.getJSONObject("source").getString("table");
                        jsonObj.put("table", table);
                        jsonObj.remove("before");
                        jsonObj.remove("source");
                        jsonObj.remove("transaction");
                        return jsonObj;
                    }
                }
        );

        // 4.2 order_cargo 表数据
        SingleOutputStreamOperator<JSONObject> orderCargoMappedStream = orderCargoStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        String table = jsonObj.getJSONObject("source").getString("table");
                        jsonObj.put("table", table);
                        jsonObj.remove("before");
                        jsonObj.remove("source");
                        jsonObj.remove("transaction");
                        return jsonObj;
                    }
                }
        );

        // TODO 5. 按照 order_id 分组
        // 5.1 order_info 表数据
        KeyedStream<JSONObject, String> orderInfoKeyedStream =
                orderInfoMappedStream.keyBy(r -> r.getJSONObject("after").getString("id"));
        // 5.2 order_cargo 表数据
        KeyedStream<JSONObject, String> orderCargoKeyedStream = 
                orderCargoMappedStream.keyBy(r -> r.getJSONObject("after").getString("order_id"));

        // TODO 6. 双流 join
        ConnectedStreams<JSONObject, JSONObject> connectedStream 
                = orderInfoKeyedStream.connect(orderCargoKeyedStream);
        
        // TODO 7. 处理关联数据
 /*       connectedStream.process(
                new CoProcessFunction<JSONObject, JSONObject, >() {
                }
        )*/

        env.execute();
    }
}
