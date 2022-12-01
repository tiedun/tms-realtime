package com.atguigu.tms.realtime.util;

import com.atguigu.tms.realtime.bean.DwsTradeOrgOrderDayBean;
import com.atguigu.tms.realtime.bean.TransientSink;
import com.atguigu.tms.realtime.common.TmsConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        int skipNum = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            declaredField.setAccessible(true);
                            try {
                                Object value = declaredField.get(obj);
                                preparedStatement.setObject(i + 1 - skipNum, value);
                            } catch (IllegalAccessException e) {
                                System.out.println("ClickHouse 数据插入 SQL 占位符传参异常 ~");
                                e.printStackTrace();
                            }
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5000L)
                        .withBatchSize(5000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(TmsConfig.CLICKHOUSE_DRIVER)
                        .withUrl(TmsConfig.CLICKHOUSE_URL)
                        .build()
        );
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("1")
                        .map(
                                new MapFunction<String, DwsTradeOrgOrderDayBean>() {
                                    @Override
                                    public DwsTradeOrgOrderDayBean map(String value) throws Exception {
                                        return DwsTradeOrgOrderDayBean.builder()
                                                .orderAmountBase(BigDecimal.ZERO)
                                                .orgId("1")
                                                .orderCountBase(1L)
                                                .ts(1L)
                                                .complexId("1")
                                                .courierEmpId("2")
                                                .orgName("2")
                                                .orderId("2")
                                                .cityId("1")
                                                .curDate("2022-07-23 11:50:23")
                                                .cityName("12")
                                                .build();

                                    }
                                }
                        )
                                .addSink(
                                        ClickHouseUtil
                                                .<DwsTradeOrgOrderDayBean>getJdbcSink(
                                                        "insert into dws_trade_org_order_day_base values(?,?,?,?,?,?,?,?")
                                );

        env.execute();
    }
}
