package com.atguigu.tms.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.atguigu.tms.realtime.common.TmsConfig;

import java.sql.Connection;
import java.sql.SQLException;

public class DruidDSUtil {

    private static DruidDataSource phoenixDataSource;

    static {
        // 创建连接池
        phoenixDataSource = new DruidDataSource();
        // 设置驱动全类名
        phoenixDataSource.setDriverClassName(TmsConfig.PHOENIX_DRIVER);
        // 设置连接 url
        phoenixDataSource.setUrl(TmsConfig.PHOENIX_SERVER);
        // 设置初始化连接池时池中连接的数量
        phoenixDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数
        phoenixDataSource.setMaxActive(20);
        // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
        phoenixDataSource.setMinIdle(1);
        // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
        phoenixDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的 SQL 语句
        phoenixDataSource.setValidationQuery("select 1");
        // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
        // 注意，默认值为 true，如果没有设置 validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        phoenixDataSource.setTestWhileIdle(true);
        // 借出连接时，是否测试，设置为 true，不测试则可能拿到失效连接导致后续处理报错
        phoenixDataSource.setTestOnBorrow(true);
        // 归还连接时，是否测试
        phoenixDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        phoenixDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        phoenixDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);
    }

    public static Connection getPhoenixConn() throws SQLException {
        return phoenixDataSource.getConnection().getConnection();
    }
}