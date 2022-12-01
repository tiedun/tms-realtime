package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.*;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.mortbay.log.Log;

import java.util.ArrayList;
import java.util.Collections;

public class DwdOrderRelevantApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 读取事实数据
        String topic = "tms_ods";
        String groupId = "dwd_order_relevant_app";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 筛选订单和订单明细数据
        SingleOutputStreamOperator<String> filteredStream = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        return table.equals("order_info") ||
                                table.equals("order_cargo");
                    }
                }
        );

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> orderInfoMappedStream = filteredStream.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        String table = jsonObj.getJSONObject("source").getString("table");
                        jsonObj.put("table", table);
                        jsonObj.remove("source");
                        jsonObj.remove("transaction");
                        return jsonObj;
                    }
                }
        );

        // TODO 5. 按照 order_id 分组
        KeyedStream<JSONObject, String> keyedStream = orderInfoMappedStream.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObj) throws Exception {
                        String table = jsonObj.getString("table");
                        if ("order_info".equals(table)) {
                            return jsonObj.getJSONObject("after").getString("id");
                        }
                        return jsonObj.getJSONObject("after").getString("order_id");
                    }
                }
        );

        // TODO 6. 定义侧输出流
        // 支付成功明细流标签
        OutputTag<String> paySucTag = new OutputTag<String>("dwd_trade_pay_suc_detail") {
        };
        // 取消运单明细流标签
        OutputTag<String> cancelDetailTag = new OutputTag<String>("dwd_trade_cancel_detail") {
        };
        // 揽收明细流标签
        OutputTag<String> receiveDetailTag = new OutputTag<String>("dwd_trans_receive_detail") {
        };
        // 发单明细流标签
        OutputTag<String> dispatchDetailTag = new OutputTag<String>("dwd_trans_dispatch_detail") {
        };
        OutputTag<String> boundFinishDetailTag = new OutputTag<String>("dwd_trans_bound_finish_detail") {
        };
        // 派送成功明细流标签
        OutputTag<String> deliverSucDetailTag = new OutputTag<String>("dwd_trans_deliver_detail") {
        };
        // 签收明细流标签
        OutputTag<String> signDetailTag = new OutputTag<String>("dwd_trans_sign_detail") {
        };

        // TODO 7. 处理数据
        SingleOutputStreamOperator<String> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, String>() {

                    private ListState<DwdOrderDetailOriginBean> detailBeansState;

                    private ValueState<DwdOrderInfoOriginBean> infoBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        detailBeansState = getRuntimeContext().getListState(
                                new ListStateDescriptor<DwdOrderDetailOriginBean>("detail_beans_state", DwdOrderDetailOriginBean.class)
                        );
                        ValueStateDescriptor<DwdOrderInfoOriginBean> infoBeanStateDescriptor
                                = new ValueStateDescriptor<>("info_bean_state", DwdOrderInfoOriginBean.class);
                        infoBeanStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60L)).build());
                        infoBeanState = getRuntimeContext().getState(
                                infoBeanStateDescriptor
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context context, Collector<String> out) throws Exception {
                        String table = jsonObj.getString("table");
                        String op = jsonObj.getString("op");
                        JSONObject data = jsonObj.getJSONObject("after");
                        switch (table) {
                            case "order_info":
                                DwdOrderInfoOriginBean infoOriginBean = data.toJavaObject(DwdOrderInfoOriginBean.class);

                                if (op.equals("c")) {
                                    infoBeanState.update(infoOriginBean);
                                    Iterable<DwdOrderDetailOriginBean> dwdOrderDetailOriginBeans = detailBeansState.get();
                                    for (DwdOrderDetailOriginBean detailOriginBean : dwdOrderDetailOriginBeans) {
                                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = new DwdTradeOrderDetailBean();
                                        dwdTradeOrderDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                        out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                                    }
                                } else if (op.equals("u")) {
                                    JSONObject oldData = jsonObj.getJSONObject("before");
                                    String oldStatus = oldData.getString("status");
                                    String status = infoOriginBean.getStatus();
                                    Iterable<DwdOrderDetailOriginBean> dwdOrderDetailOriginBeans = detailBeansState.get();
                                    if (!oldStatus.equals(status)) {
                                        String changeLog = oldStatus + " -> " + status;
                                        switch (changeLog) {
                                            case "60010 -> 60020":
                                                // 处理支付成功数据
                                                for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                    DwdTradePaySucDetailBean dwdTradePaySucDetailBean = new DwdTradePaySucDetailBean();
                                                    dwdTradePaySucDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                    context.output(paySucTag, JSON.toJSONString(dwdTradePaySucDetailBean));
                                                }
                                                break;
                                            case "60020 -> 60030":
                                                // 处理揽收明细数据
                                                for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                    DwdTransReceiveDetailBean dwdTransReceiveDetailBean = new DwdTransReceiveDetailBean();
                                                    dwdTransReceiveDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                    context.output(receiveDetailTag, JSON.toJSONString(dwdTransReceiveDetailBean));
                                                }
                                                break;
                                            case "60040 -> 60050":
                                                // 处理发单明细数据
                                                for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                    DwdTransDispatchDetailBean dispatchDetailBean = new DwdTransDispatchDetailBean();
                                                    dispatchDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                    context.output(dispatchDetailTag, JSON.toJSONString(dispatchDetailBean));
                                                }
                                                break;
                                            case "60050 -> 60060":
                                                // 处理转运完成明细数据
                                                for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                    DwdTransBoundFinishDetailBean boundFinishDetailBean = new DwdTransBoundFinishDetailBean();
                                                    boundFinishDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                    context.output(boundFinishDetailTag, JSON.toJSONString(boundFinishDetailBean));
                                                }
                                                break;
                                            case "60060 -> 60070":
                                                // 处理派送成功数据
                                                for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                    DwdTransDeliverSucDetailBean dwdTransDeliverSucDetailBean = new DwdTransDeliverSucDetailBean();
                                                    dwdTransDeliverSucDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                    context.output(deliverSucDetailTag, JSON.toJSONString(dwdTransDeliverSucDetailBean));
                                                }
                                                break;
                                            case "60070 -> 60080":
                                                // 处理签收明细数据
                                                for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                    DwdTransSignDetailBean dwdTransSignDetailBean = new DwdTransSignDetailBean();
                                                    dwdTransSignDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                    context.output(signDetailTag, JSON.toJSONString(dwdTransSignDetailBean));
                                                }
                                                // 签收后订单数据不会再发生变化，状态可以清除
                                                detailBeansState.clear();
                                                break;
                                            default:
                                                if (status.equals("60999")) {
                                                    for (DwdOrderDetailOriginBean dwdOrderDetailOriginBean : dwdOrderDetailOriginBeans) {
                                                        DwdTradeCancelDetailBean dwdTradeCancelDetailBean = new DwdTradeCancelDetailBean();
                                                        dwdTradeCancelDetailBean.mergeBean(dwdOrderDetailOriginBean, infoOriginBean);
                                                        context.output(cancelDetailTag, JSON.toJSONString(dwdTradeCancelDetailBean));
                                                    }
                                                    // 取消后订单数据不会再发生变化，状态可以清除
                                                    detailBeansState.clear();
                                                }
                                                break;
                                        }
                                    }
                                }
                                break;
                            case "order_cargo":
                                DwdOrderDetailOriginBean detailOriginBean = data.toJavaObject(DwdOrderDetailOriginBean.class);
                                if (op.equals("c")) {
                                    detailBeansState.add(detailOriginBean);
                                    DwdOrderInfoOriginBean infoOriginBeanInState = infoBeanState.value();
                                    if (infoOriginBeanInState != null) {
                                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = new DwdTradeOrderDetailBean();
                                        dwdTradeOrderDetailBean.mergeBean(detailOriginBean, infoOriginBeanInState);
                                        out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                                    }
                                }
                                break;

                        }
                    }
                }
        );

        // TODO 8. 提取侧输出流
        // 8.1 支付成功明细流
        DataStream<String> paySucStream = processedStream.getSideOutput(paySucTag);
        // 8.2 取消运单明细流
        DataStream<String> cancelDetailStream = processedStream.getSideOutput(cancelDetailTag);
        // 8.3 揽收明细流
        DataStream<String> receiveDetailStream = processedStream.getSideOutput(receiveDetailTag);
        // 8.4 发单明细流
        DataStream<String> dispatchDetailStream = processedStream.getSideOutput(dispatchDetailTag);
        // 8.5 转运成功明细流
        DataStream<String> boundFinishDetailStream = processedStream.getSideOutput(boundFinishDetailTag);
        // 8.6 派送成功明细流
        DataStream<String> deliverSucDetailStream = processedStream.getSideOutput(deliverSucDetailTag);
        // 8.7 签收明细流
        DataStream<String> signDetailStream = processedStream.getSideOutput(signDetailTag);
        paySucStream.print("paySucStream >>> ");
        cancelDetailStream.print("cancelDetailStream >>> ");
        receiveDetailStream.print("receiveDetailStream >>> ");
        dispatchDetailStream.print("dispatchDetailStream >>> ");
        boundFinishDetailStream.print("boundFinishDetailStream >>> ");
        deliverSucDetailStream.print("deliverDetailStream >>> ");
        signDetailStream.print("signDetailStream >>> ");

        // TODO 9. 发送到 Kafka 指定主题
        // 9.1 定义主题名称
        // 9.1.1 运单明细主题
        String detailTopic = "tms_dwd_trade_order_detail";
        // 9.1.2 支付成功明细主题
        String paySucDetailTopic = "tms_dwd_trade_pay_suc_detail";
        // 9.1.3 取消运单明细主题
        String cancelDetailTopic = "tms_dwd_trade_cancel_detail";
        // 9.1.4 揽收明细主题
        String receiveDetailTopic = "tms_dwd_trans_receive_detail";
        // 9.1.5 发单明细主题
        String dispatchDetailTopic = "tms_dwd_trans_dispatch_detail";
        // 9.1.6 转运完成明细主题
        String boundFinishDetailTopic = "tms_dwd_trans_bound_finish_detail";
        // 9.1.7 派送成功明细主题
        String deliverSucDetailTopic = "tms_dwd_trans_deliver_detail";
        // 9.1.8 签收明细主题
        String signDetailTopic = "tms_dwd_trans_sign_detail";

        // 9.2 发送数据到 Kafka
        // 9.2.1 运单明细数据
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(detailTopic, args);
        processedStream.addSink(kafkaProducer);

        // 9.2.2 支付成功明细数据
        FlinkKafkaProducer<String> paySucKafkaProducer = KafkaUtil.getKafkaProducer(paySucDetailTopic, args);
        paySucStream.addSink(paySucKafkaProducer);

        // 9.2.3 取消运单明细数据
        FlinkKafkaProducer<String> cancelKafkaProducer = KafkaUtil.getKafkaProducer(cancelDetailTopic, args);
        cancelDetailStream.addSink(cancelKafkaProducer);

        // 9.2.4 揽收明细数据
        FlinkKafkaProducer<String> receiveKafkaProducer = KafkaUtil.getKafkaProducer(receiveDetailTopic, args);
        receiveDetailStream.addSink(receiveKafkaProducer);

        // 9.2.5 发单明细数据
        FlinkKafkaProducer<String> dispatchKafkaProducer = KafkaUtil.getKafkaProducer(dispatchDetailTopic, args);
        dispatchDetailStream.addSink(dispatchKafkaProducer);

        // 9.2.6 转运完成明细主题
        FlinkKafkaProducer<String> boundFinishKafkaProducer = KafkaUtil.getKafkaProducer(boundFinishDetailTopic, args);
        boundFinishDetailStream.addSink(boundFinishKafkaProducer);

        // 9.2.7 派送成功明细数据
        FlinkKafkaProducer<String> deliverSucKafkaProducer = KafkaUtil.getKafkaProducer(deliverSucDetailTopic, args);
        deliverSucDetailStream.addSink(deliverSucKafkaProducer);

        // 9.2.8 签收明细数据
        FlinkKafkaProducer<String> signKafkaProducer = KafkaUtil.getKafkaProducer(signDetailTopic, args);
        signDetailStream.addSink(signKafkaProducer);

        env.execute();
    }
}

