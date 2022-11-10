package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.bean.DwdOrderDetailOriginBean;
import com.atguigu.tms.realtime.bean.DwdOrderInfoOriginBean;
import com.atguigu.tms.realtime.bean.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
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

public class DwdOrderRelevantApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

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
                        jsonObj.remove("before");
                        jsonObj.remove("source");
                        jsonObj.remove("transaction");
                        return jsonObj;
                    }
                }
        );

        orderInfoMappedStream.print("orderInfoMappedStream >>>");

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
//        OutputTag<String> lateDetailOriginTag =
//                new OutputTag<String>("late_origin_detail") {
//                };


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
                                        System.out.println("detailOriginBean = " + detailOriginBean);
                                        dwdTradeOrderDetailBean.mergeBean(detailOriginBean, infoOriginBean);
                                        out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                                    }
                                }
                                break;
                            case "order_cargo":
                                DwdOrderDetailOriginBean detailOriginBean = data.toJavaObject(DwdOrderDetailOriginBean.class);
                                switch (op) {
                                    case "c":
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

        // TODO 9. 发送到 Kafka 指定主题
        String detailTopic = "tms_dwd_trade_order_detail";
        FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.getKafkaProducer(detailTopic, args);
        processedStream.addSink(kafkaProducer);

        env.execute();
    }
}
