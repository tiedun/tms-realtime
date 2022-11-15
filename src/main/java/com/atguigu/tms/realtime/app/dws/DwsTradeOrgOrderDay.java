package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.bean.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.util.CreateEnvUtil;
import com.atguigu.tms.realtime.util.KafkaUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId, args);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据格式
        SingleOutputStreamOperator<DwdTradeOrderDetailBean> mappedStream =
                source.map(r -> JSON.parseObject(r, DwdTradeOrderDetailBean.class));

        // TODO 4. 关联机构信息
        SingleOutputStreamOperator<DwdTradeOrderDetailBean> withCourierIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwdTradeOrderDetailBean>("dim_express_courier_complex") {
                    @Override
                    public void join(DwdTradeOrderDetailBean bean, JSONObject dimJsonObj) throws Exception {

                    }

                    @Override
                    public Object getCondition(DwdTradeOrderDetailBean bean) {
                        String senderComplexId = bean.getSenderComplexId();
                        return Tuple2.of("complex_id", senderComplexId);
                    }
                },
                5 * 60,
                TimeUnit.SECONDS
        );


        env.execute();
    }
}
