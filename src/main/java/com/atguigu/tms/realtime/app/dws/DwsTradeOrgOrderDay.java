package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.bean.DwsTradeOrgOrderDayBean;
import com.atguigu.tms.realtime.util.ClickHouseUtil;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeOrgOrderDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 初始化表处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trade_order_detail 主题读取数据
        String topic = "tms_dwd_trade_order_detail";
        String groupId = "dws_trade_org_order_day";

        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据格式
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> mappedStream =
                source.map(new MapFunction<String, DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean map(String jsonStr) {
                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = JSON.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
                        return DwsTradeOrgOrderDayBean.builder()
                                .orderId(dwdTradeOrderDetailBean.getOrderId())
                                .senderDistrictId(dwdTradeOrderDetailBean.getSenderDistrictId())
                                .cityId(dwdTradeOrderDetailBean.getSenderCityId())
                                .orderAmountBase(dwdTradeOrderDetailBean.getAmount())
                                // 右移八小时
                                .ts(dwdTradeOrderDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                                .build();
                    }
                });

        // TODO 4. 关联机构信息
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withOrgIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setOrgId(dimJsonObj.getString("id".toUpperCase()));
                        bean.setOrgName(dimJsonObj.getString("org_name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTradeOrgOrderDayBean bean) {
                        return Tuple2.of("region_id", bean.getSenderDistrictId());
                    }
                }, 5 * 60,
                TimeUnit.SECONDS
        ).uid("with_org_info_stream");

        // TODO 5. 统计订单数和订单金额
        KeyedStream<DwsTradeOrgOrderDayBean, String> keyedByOrderIdStream =
                withOrgIdStream.keyBy(DwsTradeOrgOrderDayBean::getOrderId);
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withOrderCount = keyedByOrderIdStream.process(
                new KeyedProcessFunction<String, DwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean>() {

                    private ValueState<Boolean> isCountedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("is-counted", Boolean.class);
                        // 设置 TTL 避免状态常驻，消耗资源
                        // 通常同一订单明细数据的生成时间相差不会太大，ttl 设置为 5min 足矣
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(
                                org.apache.flink.api.common.time.Time.seconds(5 * 60L)).build());
                        isCountedState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(DwsTradeOrgOrderDayBean bean, Context context, Collector<DwsTradeOrgOrderDayBean> out) throws Exception {
                        Boolean isCounted = isCountedState.value();
                        if (isCounted == null) {
                            bean.setOrderCountBase(1L);
                            isCountedState.update(true);
                            out.collect(bean);
                        }
                    }
                }
        ).uid("counted_order_count_stream");

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withWatermarkStream = withOrderCount.assignTimestampsAndWatermarks(WatermarkStrategy
                .<DwsTradeOrgOrderDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<DwsTradeOrgOrderDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeOrgOrderDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }
                )).uid("watermark_stream");

        // TODO 7. 按照机构ID分组
        KeyedStream<DwsTradeOrgOrderDayBean, String> keyedStream =
                withWatermarkStream.keyBy(DwsTradeOrgOrderDayBean::getOrgId);

        // TODO 8. 开窗
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.days(1L)));

        // TODO 9. 引入触发器
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> triggeredStream = windowStream.trigger(
                new MyTriggerFunction<DwsTradeOrgOrderDayBean>()
        );

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> aggregatedStream = triggeredStream.aggregate(
                new MyAggregationFunction<DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean add(DwsTradeOrgOrderDayBean value, DwsTradeOrgOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setOrderCountBase(
                                value.getOrderCountBase() + accumulator.getOrderCountBase()
                        );
                        accumulator.setOrderAmountBase(
                                value.getOrderAmountBase().add(accumulator.getOrderAmountBase())
                        );
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeOrgOrderDayBean> elements, Collector<DwsTradeOrgOrderDayBean> out) throws Exception {
                        // 将窗口起始时间格式化为 yyyy-mm-dd HH:mm:ss 格式的日期字符串
                        // 左移八小时
                        long stt = context.window().getStart() - 8 * 60 * 60 * 1000L;
                        String curDate = DateFormatUtil.toDate(stt);

                        // 补充日期字段，修改时间戳字段，并发送到下游
                        for (DwsTradeOrgOrderDayBean element : elements) {
                            // 补充curDate字段
                            element.setCurDate(curDate);
                            // 将时间戳置为系统时间用于去重
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 11. 补充维度信息
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> fullStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCityName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTradeOrgOrderDayBean bean) {
                        return bean.getCityId();
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // TODO 12. 写出到 ClickHouse
        fullStream.addSink(ClickHouseUtil.<DwsTradeOrgOrderDayBean>getJdbcSink(
                        "insert into dws_trade_org_order_day_base values(?,?,?,?,?,?,?,?)"))
                .uid("clickhouse_stream");

        env.execute();
    }
}
