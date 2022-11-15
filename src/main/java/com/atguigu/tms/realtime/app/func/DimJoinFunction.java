package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimJsonObj) throws Exception;

    //获取维度主键的方法
    Object getCondition(T obj);
}
