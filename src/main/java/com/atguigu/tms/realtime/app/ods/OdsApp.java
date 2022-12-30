package com.atguigu.tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class OdsApp {
    public static void main(String[] args) throws Exception {

        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // 禁用算子链优化，便于调试定位问题，部署前应注释
        // env.disableOperatorChaining();

        // TODO 2. 处理离线维度数据
        String offlineDimOption = "offline_dim";
        String offlineDimServerId = "6020";
        String offlineDimSourceName = "ods_offline_dim_source";
        sinkToKafka(offlineDimOption, offlineDimServerId, offlineDimSourceName, env, args);

        // TODO 3. 处理事实数据（离线实时共用）
        String dwdOption = "dwd";
        String dwdServerId = "6030";
        String dwdSourceName = "ods_dwd_source";
        sinkToKafka(dwdOption, dwdServerId, dwdSourceName, env, args);

        // TODO 4. 处理实时维度数据
        String realtimeDimOption = "realtime_dim";
        String realtimeDimServerId = "6040";
        String realtimeDimSourceName = "ods_realtime_dim_source";
        sinkToKafka(realtimeDimOption, realtimeDimServerId, realtimeDimSourceName, env, args);

        env.execute();
    }

    public static void sinkToKafka(
            String option, String serverId, String sourceName, StreamExecutionEnvironment env, String[] args) {
        // 1. 读取数据
        MySqlSource<String> mysqlSource = CreateEnvUtil.getJSONSchemaMysqlSource(option, serverId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .uid(option + "_ods_app_" + sourceName)
                .setParallelism(1);

        // 2. ETL
        SingleOutputStreamOperator<String> flatMappedStream =
                source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String jsonStr, Collector<String> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            if (jsonObj.getJSONObject("after") != null
                                    && !jsonObj.getString("op").equals("d")) {
                                Long tsMs = jsonObj.getLong("ts_ms");
                                jsonObj.remove("ts_ms");
                                jsonObj.put("ts", tsMs);
                                out.collect(jsonObj.toJSONString());
                            }

                        } catch (JSONException jsonException) {
                            jsonException.printStackTrace();
                            Log.error("从Flink-CDC读取的数据解析异常" + jsonException.getMessage());
                        }
                    }
                }).setParallelism(1);

        // 3. 按照主键分组，避免数据倾斜
        KeyedStream<String, String> keyedStream = flatMappedStream.keyBy(
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        return jsonObj.getJSONObject("after").getString("id");
                    }
                }
        );

        // 4. 写入 Kafka 对应主题
        String topic = "tms_ods";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(topic, args);
        keyedStream
                .addSink(kafkaProducer)
                .uid(option + "_ods_app_sink_to_tms_ods_" + sourceName);
    }
}
