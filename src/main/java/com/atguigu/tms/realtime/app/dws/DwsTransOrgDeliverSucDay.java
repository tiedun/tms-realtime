package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdTransDeliverSucDetailBean;
import com.atguigu.tms.realtime.bean.DwsTransOrgDeliverSucDayBean;
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
import org.apache.flink.api.java.tuple.Tuple2;
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

public class DwsTransOrgDeliverSucDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trans_deliver_detail 主题读取数据
        String topic = "tms_dwd_trans_deliver_detail";
        String groupId = "dws_trans_org_deliver_suc_day";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> mappedStream = source.map(jsonStr -> {
            DwdTransDeliverSucDetailBean dwdTransDeliverSucDetailBean = JSON.parseObject(jsonStr, DwdTransDeliverSucDetailBean.class);
            return DwsTransOrgDeliverSucDayBean.builder()
                    .orderId(dwdTransDeliverSucDetailBean.getOrderId())
                    .districtId(dwdTransDeliverSucDetailBean.getSenderDistrictId())
                    .cityId(dwdTransDeliverSucDetailBean.getSenderCityId())
                    .provinceId(dwdTransDeliverSucDetailBean.getSenderProvinceId())
                    .ts(dwdTransDeliverSucDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 获取维度信息
        // 获取机构 ID
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withOrgIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_organ".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setOrgId(dimJsonObj.getString("id".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return Tuple2.of("region_id", bean.getDistrictId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_org_id_stream");

        // TODO 5. 统计派送成功次数
        // 5.1 按照 orderId 分组
        KeyedStream<DwsTransOrgDeliverSucDayBean, String> keyedByOrderIdStream =
                withOrgIdStream.keyBy(DwsTransOrgDeliverSucDayBean::getOrderId);
        // 5.2 统计派送成功次数（派送成功运单数）
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withDeliverSucCountStream = keyedByOrderIdStream.process(
                new KeyedProcessFunction<String, DwsTransOrgDeliverSucDayBean, DwsTransOrgDeliverSucDayBean>() {

                    private ValueState<Boolean> isCountedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Boolean> booleanValueStateDescriptor = new ValueStateDescriptor<>("is-counted-state", Boolean.class);
                        booleanValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(5L)).build());
                        isCountedState = getRuntimeContext().getState(booleanValueStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTransOrgDeliverSucDayBean bean, Context context, Collector<DwsTransOrgDeliverSucDayBean> out) throws Exception {
                        Boolean isCounted = isCountedState.value();
                        if (isCounted == null) {
                            isCountedState.update(true);
                            bean.setDeliverSucCountBase(1L);
                            out.collect(bean);
                        }
                    }
                }
        ).uid("with_deliver_suc_count_stream");

        // TODO 6. 设置水位线
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withWatermarkStream = withDeliverSucCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransOrgDeliverSucDayBean>forBoundedOutOfOrderness(Duration.ofDays(1L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransOrgDeliverSucDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTransOrgDeliverSucDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })

        ).uid("watermark_stream");

        // TODO 7. 按照机构 ID 分组
        KeyedStream<DwsTransOrgDeliverSucDayBean, String> keyedStream = withWatermarkStream.keyBy(DwsTransOrgDeliverSucDayBean::getOrgId);

        // TODO 8. 开窗
        WindowedStream<DwsTransOrgDeliverSucDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 9. 引入触发器
        WindowedStream<DwsTransOrgDeliverSucDayBean, String, TimeWindow> triggerStream = windowStream.trigger(new MyTriggerFunction<>());

        // TODO 10. 聚合
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransOrgDeliverSucDayBean>() {
                    @Override
                    public DwsTransOrgDeliverSucDayBean add(DwsTransOrgDeliverSucDayBean value, DwsTransOrgDeliverSucDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setDeliverSucCountBase(
                                accumulator.getDeliverSucCountBase() + value.getDeliverSucCountBase()
                        );
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgDeliverSucDayBean, DwsTransOrgDeliverSucDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTransOrgDeliverSucDayBean> elements, Collector<DwsTransOrgDeliverSucDayBean> out) throws Exception {
                        for (DwsTransOrgDeliverSucDayBean element : elements) {
                            long stt = context.window().getStart();
                            element.setCurDate(DateFormatUtil.toDate(stt - 8 * 60 * 60 * 1000L));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 11. 补全维度信息
        // 11.1 补充机构名称
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withOrgNameAndRegionIdStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_organ".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setOrgName(dimJsonObj.getString("org_name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return bean.getOrgId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_org_name_and_region_id_stream");

        // 11.2 补充城市名称
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withCityNameStream = AsyncDataStream.unorderedWait(
                withOrgNameAndRegionIdStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_region_info".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCityName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return bean.getCityId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // 11.3 补充省份名称
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> fullStream = AsyncDataStream.unorderedWait(
                withCityNameStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_region_info".toUpperCase()) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setProvinceName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return bean.getProvinceId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_province_name_stream");

        // TODO 12. 写出到 ClickHouse
        fullStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_org_deliver_suc_day_base values(?,?,?,?,?,?,?,?,?)")
        ).uid("clickhouse_stream");

        env.execute();
    }
}
