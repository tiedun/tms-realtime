package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.DwdBoundInboundBean;
import com.atguigu.tms.realtime.bean.DwdBoundOutboundBean;
import com.atguigu.tms.realtime.bean.DwdBoundSortBean;
import com.atguigu.tms.realtime.bean.DwdOrderOrgBoundOriginBean;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdBoundRelevantApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_ods 主题读取数据
        String topic = "tms_ods";
        String groupId = "dwd_bound_relevant_app";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 筛选中转相关数据
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        return table.equals("order_org_bound");
                    }
                }
        );

        // TODO 4. 定义侧输出流标签
        OutputTag<String> sortTag = new OutputTag<String>("dwd_bound_sort") {
        };
        OutputTag<String> outBoundTag = new OutputTag<String>("dwd_bound_out_bound") {
        };

        // TODO 5. 分流
        SingleOutputStreamOperator<String> processedStream = filteredStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<String> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        DwdOrderOrgBoundOriginBean before =
                                jsonObj.getObject("before", DwdOrderOrgBoundOriginBean.class);
                        DwdOrderOrgBoundOriginBean after =
                                jsonObj.getObject("after", DwdOrderOrgBoundOriginBean.class);

                        if (op.equals("c") || op.equals("u")) {
                            String id = after.getId();
                            String orderId = after.getOrderId();
                            String orgId = after.getOrgId();

                            if (op.equals("c")) {
                                Long ts = Long.parseLong(after.getInboundTime())
                                        - 8 * 60 * 60 * 1000L;
                                String inboundTime =
                                        DateFormatUtil.toYmdHms(
                                                Long.parseLong(after.getInboundTime())
                                                        - 8 * 60 * 60 * 1000L);
                                String inboundEmpId = after.getInboundEmpId();

                                DwdBoundInboundBean inboundBean = DwdBoundInboundBean.builder()
                                        .id(id)
                                        .orderId(orderId)
                                        .orgId(orgId)
                                        .inboundTime(inboundTime)
                                        .inboundEmpId(inboundEmpId)
                                        .ts(ts)
                                        .build();
                                out.collect(JSON.toJSONString(inboundBean));
                            } else {
                                // 筛选分拣操作
                                String beforeSortTime = before.getSortTime();
                                String sortTime = after.getSortTime();
                                if (beforeSortTime == null
                                        && sortTime != null) {
                                    Long ts = Long.parseLong(sortTime)
                                            - 8 * 60 * 60 * 1000L;
                                    sortTime = DateFormatUtil.toYmdHms(
                                            Long.parseLong(sortTime)
                                                    - 8 * 60 * 60 * 1000L
                                    );
                                    String sorterEmpId = after.getSorterEmpId();

                                    DwdBoundSortBean sortBean = DwdBoundSortBean.builder()
                                            .id(id)
                                            .orderId(orderId)
                                            .orgId(orgId)
                                            .sortTime(sortTime)
                                            .sorterEmpId(sorterEmpId)
                                            .ts(ts)
                                            .build();
                                    context.output(sortTag, JSON.toJSONString(sortBean));
                                }

                                // 筛选出库操作
                                String oldOutboundTime = before.getOutboundTime();
                                String outboundTime = after.getOutboundTime();
                                if (oldOutboundTime == null
                                        && outboundTime != null) {
                                    Long ts = Long.parseLong(outboundTime)
                                            - 8 * 60 * 60 * 1000L;
                                    outboundTime = DateFormatUtil.toYmdHms(
                                            Long.parseLong(outboundTime)
                                                    - 8 * 60 * 60 * 1000L
                                    );
                                    String outboundEmpId = after.getOutboundEmpId();

                                    DwdBoundOutboundBean outboundBean = DwdBoundOutboundBean.builder()
                                            .id(id)
                                            .orderId(orderId)
                                            .orgId(orgId)
                                            .outboundTime(outboundTime)
                                            .outboundEmpId(outboundEmpId)
                                            .ts(ts)
                                            .build();
                                    context.output(outBoundTag, JSON.toJSONString(outboundBean));
                                }
                            }
                        }
                    }
                }
        );

        // TODO 6. 提取侧输出流
        // 6.1 提取分拣流
        DataStream<String> sortStream = processedStream.getSideOutput(sortTag);
        // 6.2 提取出库流
        DataStream<String> outboundStream = processedStream.getSideOutput(outBoundTag);

        // TODO 7. 写出到 Kafka 指定主题
        // 中转域入库事实主题
        String inboundTopic = "tms_dwd_bound_inbound";
        // 中转域分拣事实主题
        String sortTopic = "tms_dwd_bound_sort";
        // 中转域出库事实主题
        String outboundTopic = "tms_dwd_bound_outbound";

        KafkaSink<String> inboundProducer = KafkaUtil.getKafkaProducer(inboundTopic, args);
        KafkaSink<String> sortProducer = KafkaUtil.getKafkaProducer(sortTopic, args);
        KafkaSink<String> outboundProducer = KafkaUtil.getKafkaProducer(outboundTopic, args);

        processedStream
                .sinkTo(inboundProducer)
                .uid("inbound_producer");
        sortStream
                .sinkTo(sortProducer)
                .uid("sort_producer");
        outboundStream
                .sinkTo(outboundProducer)
                .uid("outbound_producer");

        env.execute();
    }
}
