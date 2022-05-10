package com.yang.apitest.sink;

import com.yang.apitest.pojo.Topic001;
import com.yang.utils.InputDataStreamUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author zhangyang03
 * @Description 将Redis当作sink的输出对象
 * @create 2022-05-10 11:16
 */
public class SinkRedis {
    public static void main(String[] args) throws Exception {
        // flink 添加 kafka数据源
        DataStream<Topic001> topoc001DataStream = InputDataStreamUtil.getTopoc001DataStream();
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6380)
                .setPassword("123456")
                .setDatabase(0)
                .build();
        topoc001DataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        InputDataStreamUtil.execute("SinkRedis");
    }

    // 自定义 RedisMapper
    public static class MyRedisMapper implements RedisMapper<Topic001> {


        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "Topic001_num");
        }

        @Override
        public String getKeyFromData(Topic001 topic001) {
            return topic001.getId().toString();
        }

        @Override
        public String getValueFromData(Topic001 topic001) {
            return topic001.getNum().toString();
        }
    }
}
