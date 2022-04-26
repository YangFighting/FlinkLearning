package com.yang.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author zhangyang03
 * @Description 从kafka中读取数据
 * @create 2022-04-26 10:52
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);
        // 配置kafka
        String topic = "topic001";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "consumer-group");
        properties.setProperty("auto.offset.reset", "earliest");

        // flink 添加 kafka数据源
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        // 打印输出
        dataStream.print("KafkaSource");

        // 执行
        env.execute("KafkaSource");
    }

}
