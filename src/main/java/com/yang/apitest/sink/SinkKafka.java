package com.yang.apitest.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author zhangyang03
 * @Description
 * @create 2022-05-10 10:36
 */
public class SinkKafka {

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
        DataStream<String> inputDataStream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));

        // 序列化从Kafka中读取的数据
        DataStream<String> dataStream = inputDataStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()),
                    new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()),
                    Integer.valueOf(jsonObject.get("num").toString())).toString();
        });

        // 将数据写入Kafka
        dataStream.addSink(new FlinkKafkaProducer<>("localhost:9092", "sink_topic001", new SimpleStringSchema()));

        env.execute();
    }
}
