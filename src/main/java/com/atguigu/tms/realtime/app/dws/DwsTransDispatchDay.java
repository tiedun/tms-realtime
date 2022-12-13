package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdTransDispatchDetailBean;
import com.atguigu.tms.realtime.bean.DwsTransDispatchDayBean;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTransDispatchDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trans_dispatch_detail 主题读取数据
        String topic = "tms_dwd_trans_dispatch_detail";
        String groupId = "dws_trans_dispatch_day";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransDispatchDayBean> mappedStream = source.map(jsonStr -> {
            DwdTransDispatchDetailBean dispatchDetailBean = JSON.parseObject(jsonStr, DwdTransDispatchDetailBean.class);
            return DwsTransDispatchDayBean.builder()
                    .orderId(dispatchDetailBean.getOrderId())
                    .ts(dispatchDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 统计发单数
        KeyedStream<DwsTransDispatchDayBean, String> keyedStream = mappedStream.keyBy(DwsTransDispatchDayBean::getOrderId);
        SingleOutputStreamOperator<DwsTransDispatchDayBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsTransDispatchDayBean, DwsTransDispatchDayBean>() {

                    private ValueState<Boolean> isCountedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> booleanValueStateDescriptor = new ValueStateDescriptor<>("is-counted", Boolean.class);
                        booleanValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(5L)).build());
                        isCountedState = getRuntimeContext().getState(booleanValueStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTransDispatchDayBean bean, Context context, Collector<DwsTransDispatchDayBean> out) throws Exception {
                        Boolean isCounted = isCountedState.value();
                        if (isCounted == null) {
                            bean.setDispatchOrderCountBase(1L);
                            isCountedState.update(true);
                        } else {
                            bean.setDispatchOrderCountBase(0L);
                        }
                        out.collect(bean);
                    }
                }
        );

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTransDispatchDayBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransDispatchDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransDispatchDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTransDispatchDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 6. 开窗
        AllWindowedStream<DwsTransDispatchDayBean, TimeWindow> windowedStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 7. 引入触发器
        AllWindowedStream<DwsTransDispatchDayBean, TimeWindow> triggerStream = windowedStream.trigger(
                new MyTriggerFunction<DwsTransDispatchDayBean>()
        );

        // TODO 8. 聚合
        SingleOutputStreamOperator<DwsTransDispatchDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransDispatchDayBean>() {
                    @Override
                    public DwsTransDispatchDayBean add(DwsTransDispatchDayBean value, DwsTransDispatchDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setDispatchOrderCountBase(
                                accumulator.getDispatchOrderCountBase() + value.getDispatchOrderCountBase()
                        );
                        return accumulator;
                    }
                },
                new ProcessAllWindowFunction<DwsTransDispatchDayBean, DwsTransDispatchDayBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTransDispatchDayBean> elements, Collector<DwsTransDispatchDayBean> out) throws Exception {
                        for (DwsTransDispatchDayBean element : elements) {
                            String curDate = DateFormatUtil.toDate(context.window().getStart() - 8 * 60 * 60 * 1000L);
                            // 补充统计日期字段
                            element.setCurDate(curDate);
                            // 补充时间戳字段
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 9. 写出到 ClickHouse
        aggregatedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_dispatch_day_base values(?,?,?)")
        );

        env.execute();
    }
}
