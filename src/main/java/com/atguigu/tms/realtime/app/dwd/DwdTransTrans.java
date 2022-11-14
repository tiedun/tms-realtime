package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.DwdTransTransBean;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class DwdTransTrans {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_ods 主题读取数据
        String topic = "tms_ods";
        String groupId = "dwd_trans_trans";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 筛选运输完成数据
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        if (!table.equals("transport_task")) {
                            return false;
                        }

                        String op = jsonObj.getString("op");
                        JSONObject before = jsonObj.getJSONObject("before");
                        if (before == null) {
                            return false;
                        }
                        JSONObject after = jsonObj.getJSONObject("after");
                        String oldActualEndTime = before.getString("actual_end_time");
                        String actualEndTime = after.getString("actual_end_time");
                        return op.equals("u") &&
                                oldActualEndTime == null &&
                                actualEndTime != null;
                    }
                }
        );

        // TODO 4. 转换数据结构，补全时间戳字段
        SingleOutputStreamOperator<String> processedStream = filteredStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<String> out) throws Exception {
                        // 获取修改后的数据并转换数据类型
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        DwdTransTransBean dwdTransTransBean =
                                jsonObj.getObject("after", DwdTransTransBean.class);

                        // 补全时间戳字段
                        dwdTransTransBean.setTs(
                                Long.parseLong(dwdTransTransBean.getActualEndTime())
                                        - 8 * 60 * 60 * 1000L
                        );

                        // 补全运输时间字段
                        dwdTransTransBean.setTransportTime(
                                Long.parseLong(dwdTransTransBean.getActualEndTime())
                                        - Long.parseLong(dwdTransTransBean.getActualStartTime())
                        );

                        // 处理时区问题
                        dwdTransTransBean.setActualStartTime(
                                DateFormatUtil.toYmdHms(
                                        Long.parseLong(dwdTransTransBean.getActualStartTime())
                                                - 8 * 60 * 60 * 1000L));

                        dwdTransTransBean.setActualEndTime(
                                DateFormatUtil.toYmdHms(
                                        Long.parseLong(dwdTransTransBean.getActualEndTime())
                                                - 8 * 60 * 60 * 1000L));

                        out.collect(JSON.toJSONString(dwdTransTransBean));
                    }
                }
        );

        // TODO 5. 写出到 Kafka dwd_trans_trans 主题
        String sinkTopic = "tms_dwd_trans_trans";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(sinkTopic, args);
        processedStream.addSink(kafkaProducer);

        env.execute();
    }
}
