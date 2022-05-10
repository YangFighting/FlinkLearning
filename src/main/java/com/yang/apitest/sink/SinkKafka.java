package com.yang.apitest.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import com.yang.utils.InputDataStreamUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;

/**
 * @author zhangyang03
 * @Description
 * @create 2022-05-10 10:36
 */
public class SinkKafka {

    public static void main(String[] args) throws Exception {
        // flink 添加 kafka数据源
        DataStream<String> inputDataStream = InputDataStreamUtil.getDataStreamFromKafka();

        // 序列化从Kafka中读取的数据
        DataStream<String> dataStream = inputDataStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()),
                    new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()),
                    Integer.valueOf(jsonObject.get("num").toString())).toString();
        });

        // 将数据写入Kafka
        dataStream.addSink(new FlinkKafkaProducer<>("localhost:9092", "sink_topic001", new SimpleStringSchema()));

        InputDataStreamUtil.execute("SinkKafka");
    }
}
