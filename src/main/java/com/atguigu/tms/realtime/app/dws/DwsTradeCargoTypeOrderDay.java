package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.bean.DwsTradeCargoTypeOrderDayBean;
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
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeCargoTypeOrderDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka 指定主题消费数据
        String topic = "tms_dwd_trade_order_detail";
        String groupId = "dws_trade_cargo_type_order_day";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> mappedStream = source.map(jsonStr -> {
            DwdTradeOrderDetailBean bean = JSON.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
            return DwsTradeCargoTypeOrderDayBean.builder()
                    .cargoType(bean.getCargoType())
                    .orderAmountBase(bean.getAmount())
                    .orderId(bean.getOrderId())
                    .ts(bean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 统计订单数和订单金额
        // 4.1 按照订单 ID 分组
        KeyedStream<DwsTradeCargoTypeOrderDayBean, String> keyedStream = mappedStream.keyBy(DwsTradeCargoTypeOrderDayBean::getOrderId);

        // 4.2 统计订单数和订单金额
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, DwsTradeCargoTypeOrderDayBean, DwsTradeCargoTypeOrderDayBean>() {

                    private ValueState<Boolean> isCountedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 设置 TTL 避免状态常驻，消耗资源
                        // 通常同一订单明细数据的生成时间相差不会太大，ttl 设置为 5min 足矣
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("is-counted", Boolean.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5 * 60L)).build());
                        isCountedState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(DwsTradeCargoTypeOrderDayBean bean, Context context, Collector<DwsTradeCargoTypeOrderDayBean> out) throws Exception {
                        Boolean isCounted = isCountedState.value();
                        if (isCounted == null) {
                            bean.setOrderCountBase(1L);
                            isCountedState.update(true);
                            out.collect(bean);
                        }
                    }
                }
        ).uid("count_order_count_stream");

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeCargoTypeOrderDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCargoTypeOrderDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeCargoTypeOrderDayBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        })

        ).uid("watermark_stream");

        // TODO 6. 按照货物类别分组
        KeyedStream<DwsTradeCargoTypeOrderDayBean, String> keyedByCargoTypeStream = withWatermarkStream.keyBy(DwsTradeCargoTypeOrderDayBean::getCargoType);

        // TODO 7. 开窗
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> windowStream = keyedByCargoTypeStream
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 8. 引入触发器
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> withTriggerStream = windowStream.trigger(
                new MyTriggerFunction<DwsTradeCargoTypeOrderDayBean>()
        );

        // TODO 9. 聚合
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> aggregatedStream = withTriggerStream.aggregate(
                new MyAggregationFunction<DwsTradeCargoTypeOrderDayBean>() {
                    @Override
                    public DwsTradeCargoTypeOrderDayBean add(DwsTradeCargoTypeOrderDayBean value, DwsTradeCargoTypeOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setOrderAmountBase(
                                value.getOrderAmountBase().add(accumulator.getOrderAmountBase()));
                        accumulator.setOrderCountBase(
                                value.getOrderCountBase() + accumulator.getOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeCargoTypeOrderDayBean, DwsTradeCargoTypeOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTradeCargoTypeOrderDayBean> elements, Collector<DwsTradeCargoTypeOrderDayBean> out) throws Exception {
                        long stt = context.window().getStart() - 8 * 60 * 60 * 1000L;
                        String curDate = DateFormatUtil.toDate(stt);
                        for (DwsTradeCargoTypeOrderDayBean element : elements) {
                            element.setCurDate(curDate);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 10. 补充货物类型字段
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> fullStream = AsyncDataStream.unorderedWait(aggregatedStream,
                new DimAsyncFunction<DwsTradeCargoTypeOrderDayBean>("dim_base_dic") {
                    @Override
                    public void join(DwsTradeCargoTypeOrderDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCargoTypeName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTradeCargoTypeOrderDayBean bean) {
                        return bean.getCargoType();
                    }
                }, 5 * 60L,
                TimeUnit.SECONDS).uid("with_cargo_type_name_stream");

        // TODO 11. 写入 ClickHouse
        fullStream.addSink(ClickHouseUtil.getJdbcSink(
                "insert into dws_trade_cargo_type_order_day_base values(?,?,?,?,?,?)"
        )).uid("clickhouse_sink");


        env.execute();
    }
}
