package com.atguigu.tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import com.google.gson.JsonObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * description:
 * Created by 铁盾 on 2022/11/8
 */
public class OdsDimApp {
    public static void main(String[] args) throws Exception {

        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // TODO 2. 读取维度数据
        String option = "dim";
        String serverId = "6010-6020";
        MySqlSource<String> mysqlSource = CreateEnvUtil.getJSONSchemaMysqlSource(option, serverId, args);
        DataStreamSource<String> source = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "ods_dim_app");

        // TODO 3. ETL
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = null;
                        try {
                            jsonObj = JSON.parseObject(jsonStr);
                            return jsonObj.getJSONObject("after") != null
                                    && !jsonObj.getString("op").equals("d");
                        } catch (Exception e) {
                            e.printStackTrace();
                            return false;
                        }
                    }
                }
        );

        // TODO 4. 写入 Kafka  ods_tms_dim 主题
        String topic = "ods_tms_dim";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(topic, args);
        filteredStream.addSink(kafkaProducer);

        env.execute();
    }
}
