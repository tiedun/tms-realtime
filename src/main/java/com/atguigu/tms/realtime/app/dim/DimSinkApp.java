package com.atguigu.tms.realtime.app.dim;

import com.atguigu.tms.realtime.util.CreateEnvUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimSinkApp {
    public static void main(String[] args) {
        // TODO 1. 获取流处理环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // TODO 2. 读取
        String topic = "tms_ods";
        String groupId = "tms_dim_sink_app";




    }
}
