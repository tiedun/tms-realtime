package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.bean.DwdBoundSortBean;
import com.atguigu.tms.realtime.bean.DwsBoundOrgSortDayBean;
import com.atguigu.tms.realtime.util.ClickHouseUtil;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.DateFormatUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsBoundOrgSortDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_bound_sort 主题读取数据
        String topic = "tms_dwd_bound_sort";
        String groupId = "dws_bound_org_sort_day";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> mappedStream = source.map(jsonStr -> {
            DwdBoundSortBean dwdBoundSortBean = JSON.parseObject(jsonStr, DwdBoundSortBean.class);
            return DwsBoundOrgSortDayBean.builder()
                    .orgId(dwdBoundSortBean.getOrgId())
                    .sortCountBase(1L)
                    .ts(dwdBoundSortBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsBoundOrgSortDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsBoundOrgSortDayBean>() {
                            @Override
                            public long extractTimestamp(DwsBoundOrgSortDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        ).uid("watermark_stream");

        // TODO 5. 按照机构 ID 分组
        KeyedStream<DwsBoundOrgSortDayBean, String> keyedStream = withWatermarkStream.keyBy(DwsBoundOrgSortDayBean::getOrgId);

        // TODO 6. 开窗
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.days(1L)));

        // TODO 7. 引入触发器
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> triggerStream =
                windowStream.trigger(new MyTriggerFunction<>());

        // TODO 8. 聚合
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> aggregatedStream = triggerStream.aggregate(
                        new MyAggregationFunction<DwsBoundOrgSortDayBean>() {
                            @Override
                            public DwsBoundOrgSortDayBean add(DwsBoundOrgSortDayBean value, DwsBoundOrgSortDayBean accumulator) {
                                if (accumulator == null) {
                                    return value;
                                }
                                accumulator.setSortCountBase(accumulator.getSortCountBase() + 1);
                                return accumulator;
                            }
                        },
                        new ProcessWindowFunction<DwsBoundOrgSortDayBean, DwsBoundOrgSortDayBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<DwsBoundOrgSortDayBean> elements, Collector<DwsBoundOrgSortDayBean> out) throws Exception {
                                for (DwsBoundOrgSortDayBean element : elements) {
                                    long stt = context.window().getStart();
                                    element.setCurDate(DateFormatUtil.toDate(stt - 8 * 60 * 60 * 1000L));
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                )
                .uid("aggregate_stream");

        // TODO 9. 关联维度信息
        // 9.1 获取机构名称及用于关联获取城市 ID 的机构 ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withOrgNameStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ".toUpperCase()) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj) {
                        // 获取上级机构 ID
                        String orgParentId = dimJsonObj.getString("org_parent_id".toUpperCase());
                        bean.setOrgName(dimJsonObj.getString("org_name".toUpperCase()));
                        bean.setJoinOrgId(orgParentId != null ? orgParentId : bean.getOrgId());
                    }

                    @Override
                    public Object getCondition(DwsBoundOrgSortDayBean bean) {
                        return bean.getOrgId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_org_name_stream");

        // 9.2 获取城市 ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityIdStream = AsyncDataStream.unorderedWait(
                withOrgNameStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ".toUpperCase()) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj) {
                        bean.setCityId(dimJsonObj.getString("region_id".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsBoundOrgSortDayBean bean) {
                        return bean.getJoinOrgId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_id_stream");

        // 9.3 获取城市名称及省份 ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityNameStream = AsyncDataStream.unorderedWait(
                withCityIdStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info".toUpperCase()) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setCityName(dimJsonObj.getString("name".toUpperCase()));
                        bean.setProvinceId(dimJsonObj.getString("parent_id".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsBoundOrgSortDayBean bean) {
                        return bean.getCityId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // 9.4 获取省份名称
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> fullStream = AsyncDataStream.unorderedWait(
                withCityNameStream,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info".toUpperCase()) {
                    @Override
                    public void join(DwsBoundOrgSortDayBean bean, JSONObject dimJsonObj) throws Exception {
                        bean.setProvinceName(dimJsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public Object getCondition(DwsBoundOrgSortDayBean bean) {
                        return bean.getProvinceId();
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_province_name_stream");

        // TODO 10. 写出到 ClickHouse
        fullStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_bound_org_sort_day_base values(?,?,?,?,?,?,?,?,?)")
        ).uid("clickhouse_sink");

        env.execute();
    }
}
