package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.util.DimUtil;
import com.atguigu.tms.realtime.util.ThreadPoolUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    // 表名
    private String tableName;

    // 线程池操作对象
    private ExecutorService executorService;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //从线程池中获取线程，发送异步请求
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 1. 根据流中的对象获取关联条件
                            Object condition = getCondition(obj);

                            // 2. 根据维度的主键获取维度对象
                            JSONObject dimJsonObj = null;
                            try {
                                if (condition instanceof String) {
                                    String conditionKey = (String) condition;
                                    dimJsonObj = DimUtil.getDimInfo(tableName, conditionKey);
                                } else if (condition instanceof Tuple2) {
                                    Tuple2<String, String> keyValuePair = (Tuple2<String, String>) condition;
                                    dimJsonObj = DimUtil.getDimInfo(tableName, keyValuePair);
                                } else if (condition instanceof Tuple2[]) {
                                    Tuple2<String, String>[] keyValuePair = (Tuple2<String, String>[]) condition;
                                    dimJsonObj = DimUtil.getDimInfo(tableName, keyValuePair);
                                } else {
                                    System.out.println("condition = " + condition);
                                    System.out.println("condition.getClass() = " + condition.getClass());
                                    throw new IllegalArgumentException("筛选条件数据类型不匹配!");
                                }

                            } catch (Exception e) {
                                System.out.println("维度数据异步查询异常");
                                e.printStackTrace();
                            }

                            // 3. 将查询出来的维度信息 补充到流中的对象属性上
                            if (dimJsonObj != null) {
                                join(obj, dimJsonObj);
                            }
                            resultFuture.complete(Collections.singleton(obj));
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("异步维度关联发生异常了");
                        }
                    }
                }
        );
    }

//    public static void main(String[] args) {
//        Object condition = null;
//        Object condition2 = null;
//        Object condition3 = null;
//        condition = "1";
//        condition2 = Tuple2.of("1", "1");
//        condition3 = new Tuple2[] {Tuple2.of("hh", "xx"), Tuple2.of("ds", "23")};
//        if (condition instanceof String) {
//            String conditionKey = (String) condition;
//            System.out.println("1");
//        } if (condition2 instanceof Tuple2) {
//            Tuple2<String, String> keyValuePair = (Tuple2<String, String>) condition2;
//            System.out.println("2");
//        } if (condition3 instanceof Tuple2[]) {
//            Tuple2<String, String>[] keyValuePair = (Tuple2<String, String>[]) condition3;
//            System.out.println("3");
//        }
//    }
}
