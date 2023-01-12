package com.atguigu.tms.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

public class JedisUtil {
    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);

        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "hadoop102", 6379, 10000);
    }

    public static Jedis getJedis() {
        // 获取Jedis客户端
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public static void main(String[] args) {
//        Jedis jedis = getJedis();
//        for (int i = 0; i < 9; i++) {
//            Jedis jedis11 = getJedis();
//            jedis11.close();
//        }
//        String pong = jedis.ping();
//        System.out.println(pong);
//        int numIdle = jedisPool.getNumIdle();
//        System.out.println("numIdle = " + numIdle);
//        int numActive = jedisPool.getNumActive();
//        System.out.println("numActive = " + numActive);
//        jedis.close();
//        int numActive1 = jedisPool.getNumActive();
//        System.out.println("numActive1 = " + numActive1);
//        int numIdle1 = jedisPool.getNumIdle();
//        System.out.println("numIdle1 = " + numIdle1);
//        Jedis jedis1 = getJedis();
//        System.out.println(jedis1.ping());
//        int numActive2 = jedisPool.getNumActive();
//        System.out.println("numActive1 = " + numActive2);
//        int numIdle2 = jedisPool.getNumIdle();
//        System.out.println("numIdle1 = " + numIdle2);

        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("dim*");
        for (String key : keys) {
            System.out.println("key = " + key);
        }
    }
}
