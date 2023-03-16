package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdTransDispatchDetailBean;
import com.atguigu.tms.realtime.bean.DwsTransBoundFinishDayBean;
import com.atguigu.tms.realtime.util.ClickHouseUtil;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTransBoundFinishDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trans_bound_finish_detail 主题读取数据
        String topic = "tms_dwd_trans_bound_finish_detail";
        String groupId = "dws_trans_bound_finish_day";

        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> mappedStream = source.map(jsonStr -> {
            DwdTransDispatchDetailBean dispatchDetailBean = JSON.parseObject(jsonStr, DwdTransDispatchDetailBean.class);
            return DwsTransBoundFinishDayBean.builder()
                    .orderId(dispatchDetailBean.getOrderId())
                    .ts(dispatchDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 统计转运完成运单数
        KeyedStream<DwsTransBoundFinishDayBean, String> keyedStream = mappedStream.keyBy(DwsTransBoundFinishDayBean::getOrderId);
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsTransBoundFinishDayBean, DwsTransBoundFinishDayBean>() {

                    private ValueState<Boolean> isCountedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> booleanValueStateDescriptor = new ValueStateDescriptor<>("is-counted", Boolean.class);
                        booleanValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(5L)).build());
                        isCountedState = getRuntimeContext().getState(booleanValueStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTransBoundFinishDayBean bean, Context context, Collector<DwsTransBoundFinishDayBean> out) throws Exception {
                        Boolean isCounted = isCountedState.value();
                        if (isCounted == null) {
                            bean.setBoundFinishOrderCountBase(1L);
                            isCountedState.update(true);
                            out.collect(bean);
                        }
                    }
                }
        );

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransBoundFinishDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransBoundFinishDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTransBoundFinishDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        ).uid("watermark_stream");

        // TODO 6. 开窗
        AllWindowedStream<DwsTransBoundFinishDayBean, TimeWindow> windowedStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 7. 引入触发器
        AllWindowedStream<DwsTransBoundFinishDayBean, TimeWindow> triggerStream = windowedStream.trigger(
                new MyTriggerFunction<DwsTransBoundFinishDayBean>()
        );

        // TODO 8. 聚合
        SingleOutputStreamOperator<DwsTransBoundFinishDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransBoundFinishDayBean>() {
                    public DwsTransBoundFinishDayBean add(DwsTransBoundFinishDayBean value, DwsTransBoundFinishDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setBoundFinishOrderCountBase(
                                accumulator.getBoundFinishOrderCountBase() + value.getBoundFinishOrderCountBase()
                        );
                        return accumulator;
                    }
                },
                new ProcessAllWindowFunction<DwsTransBoundFinishDayBean, DwsTransBoundFinishDayBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTransBoundFinishDayBean> elements, Collector<DwsTransBoundFinishDayBean> out) throws Exception {
                        for (DwsTransBoundFinishDayBean element : elements) {
                            String curDate = DateFormatUtil.toDate(context.window().getStart() - 8 * 60 * 60 * 1000L);
                            // 补充统计日期字段
                            element.setCurDate(curDate);
                            // 补充时间戳字段
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 9. 写出到 ClickHouse
        aggregatedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_bound_finish_day_base values(?,?,?)")
        ).uid("clickhouse_sink");

        env.execute();
    }
}
