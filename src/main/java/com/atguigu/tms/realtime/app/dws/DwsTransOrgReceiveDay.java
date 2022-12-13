package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdTransReceiveDetailBean;
import com.atguigu.tms.realtime.bean.DwsTransOrgReceiveDayBean;
import com.atguigu.tms.realtime.util.ClickHouseUtil;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTransOrgReceiveDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(1);

        // TODO 2. 从指定主题读取数据
        String topic = "tms_dwd_trans_receive_detail";
        String groupId = "dws_trans_org_receive_day";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> mappedStream = source.map(
                jsonStr -> {
                    DwdTransReceiveDetailBean dwdTransReceiveDetailBean = JSON.parseObject(jsonStr, DwdTransReceiveDetailBean.class);
                    return DwsTransOrgReceiveDayBean.builder()
                            .orderId(dwdTransReceiveDetailBean.getOrderId())
                            .complexId(dwdTransReceiveDetailBean.getSenderComplexId())
                            .cityId(dwdTransReceiveDetailBean.getSenderCityId())
                            .ts(dwdTransReceiveDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                            .build();
                }
        );

        // TODO 4. 关联维度信息
        // 4.1 关联快递员id
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withCourierIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_express_courier_complex") {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCourierEmpId(dimJsonObj.getString("courier_emp_id".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgReceiveDayBean bean) {
                        String senderComplexId = bean.getComplexId();
                        return Tuple2.of("complex_id", senderComplexId);
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        );

        // 4.2 关联机构id
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withOrgIdStream = AsyncDataStream.unorderedWait(
                withCourierIdStream,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_express_courier") {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setOrgId(dimJsonObj.getString("org_id".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgReceiveDayBean bean) {
                        return Tuple2.of("emp_id", bean.getCourierEmpId());
                    }
                }, 5 * 60,
                TimeUnit.SECONDS
        );

        // TODO 5. 统计揽收次数
        KeyedStream<DwsTransOrgReceiveDayBean, String> keyedByOrderIdStream = withOrgIdStream.keyBy(DwsTransOrgReceiveDayBean::getOrderId);
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withReceiveCountStream = keyedByOrderIdStream.process(
                new KeyedProcessFunction<String, DwsTransOrgReceiveDayBean, DwsTransOrgReceiveDayBean>() {

                    private ValueState<Boolean> isCountedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> booleanValueStateDescriptor = new ValueStateDescriptor<>("is-counted", Boolean.class);
                        booleanValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(5L)).build());
                        isCountedState = getRuntimeContext().getState(booleanValueStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTransOrgReceiveDayBean bean, Context context, Collector<DwsTransOrgReceiveDayBean> out) throws Exception {
                        Boolean isCounted = isCountedState.value();
                        if (isCounted == null) {
                            bean.setReceiveOrderCountBase(1L);
                        } else {
                            bean.setReceiveOrderCountBase(0L);
                        }
                        out.collect(bean);
                    }
                }
        );

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withWatermarkStream = withReceiveCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransOrgReceiveDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTransOrgReceiveDayBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTransOrgReceiveDayBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

        // TODO 7. 按照 orgID 分组
        KeyedStream<DwsTransOrgReceiveDayBean, String> keyedStream = withWatermarkStream.keyBy(DwsTransOrgReceiveDayBean::getOrgId);

        // TODO 8. 开窗
        WindowedStream<DwsTransOrgReceiveDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 9. 引入触发器
        WindowedStream<DwsTransOrgReceiveDayBean, String, TimeWindow> triggerStream = windowStream.trigger(
                new MyTriggerFunction<DwsTransOrgReceiveDayBean>()
        );

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransOrgReceiveDayBean>() {
                    @Override
                    public DwsTransOrgReceiveDayBean add(DwsTransOrgReceiveDayBean value, DwsTransOrgReceiveDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setReceiveOrderCountBase(
                                accumulator.getReceiveOrderCountBase() + value.getReceiveOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgReceiveDayBean, DwsTransOrgReceiveDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTransOrgReceiveDayBean> elements, Collector<DwsTransOrgReceiveDayBean> out) throws Exception {
                        for (DwsTransOrgReceiveDayBean element : elements) {
                            // 补全统计日期字段
                            String curDate = DateFormatUtil.toDate(context.window().getStart() - 8 * 60 * 60 * 1000L);
                            element.setCurDate(curDate);
                            // 补全时间戳
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 11. 补充维度信息
        // 11.1 补充转运站名称及地区ID
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withOrgNameStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_base_organ".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setOrgName(dimJsonObj.getString("org_name".toUpperCase()));
                        bean.setRegionId(dimJsonObj.getString("region_id".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgReceiveDayBean bean) {
                        return bean.getOrgId();
                    }
                },
                60, TimeUnit.SECONDS
        );

        // 11.2 补充地区名称
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withRegionNameStream = AsyncDataStream.unorderedWait(
                withOrgNameStream,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_base_region_info".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setRegionName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgReceiveDayBean bean) {
                        return bean.getRegionId();
                    }
                },
                60, TimeUnit.SECONDS
        );

        // 11.3 补充城市名称
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> fullStream = AsyncDataStream.unorderedWait(
                withRegionNameStream,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_base_region_info".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCityName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgReceiveDayBean bean) {
                        return bean.getCityId();
                    }
                },
                60, TimeUnit.SECONDS
        );

        // TODO 12. 写出到 ClickHouse
        fullStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_org_receive_day_base values(?,?,?,?,?,?,?,?,?)")
        );


        env.execute();
    }
}
