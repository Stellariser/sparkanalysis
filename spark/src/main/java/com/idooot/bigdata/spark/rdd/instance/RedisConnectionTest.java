package com.idooot.bigdata.spark.rdd.instance;
import redis.clients.jedis.Jedis;

public class RedisConnectionTest {
    public static void main(String[] args) {
        // 配置 Redis 主机和端口
        String redisHost = "127.0.0.1";
        int redisPort = 6379;

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            // 连接测试
            String response = jedis.ping();
            if ("PONG".equals(response)) {
                System.out.println("Redis 连接成功！");
            } else {
                System.out.println("Redis 连接失败，返回值: " + response);
            }
        } catch (Exception e) {
            System.err.println("无法连接到 Redis: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
