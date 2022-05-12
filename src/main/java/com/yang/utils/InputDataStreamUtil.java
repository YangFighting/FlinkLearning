package com.yang.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author zhangyang03
 * @Description 获取输入数据公共方法
 * @create 2022-05-10 11:19
 */
public class InputDataStreamUtil {

    private static StreamExecutionEnvironment env;
    private static final String KAFAKA_BOOTSTRAP_SERVERS = "localhost:9092";

    private InputDataStreamUtil() {
    }

    public static StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static DataStream<String> getDataStreamFromText() {
        env = getEnv();
        env.setParallelism(4);
        // 从文件中读取数据
        Properties prop = new Properties();
        prop.put("inputFilePath", "E:\\project_java\\FlinkLearning\\src\\main\\resources\\topic001.txt");
        String inputFilePath = prop.getProperty("inputFilePath");
        return env.readTextFile(inputFilePath);
    }

    /**
     *    从socket文本流获取数据
     * @return String 流
     */
    public static DataStream<String> getDataStreamFromSocket(String hostname, int port) {
        env = getEnv();
        env.setParallelism(1);
        return env.socketTextStream(hostname, port);
    }

    /**
     *  根据 String 流 转换成 Topic001 流
     * @param stringDataStream  String 流
     * @return Topic001 流
     */
    public static DataStream<Topic001> getTopoc001FromStringDataStream(DataStream<String> stringDataStream) {
        return stringDataStream.filter(JsonUtil::isJson).map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()), new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()), Integer.valueOf(jsonObject.get("num").toString()));
        });

    }

    public static DataStream<Topic001> getTopoc001DataStream() {
        DataStream<String> dataStream = getDataStreamFromText();
        return dataStream.filter(JsonUtil::isJson).map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()), new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()), Integer.valueOf(jsonObject.get("num").toString()));
        });
    }

    public static DataStream<String> getDataStreamFromKafka() {
        // 创建执行环境
        env = getEnv();

        // 设置并行度
        env.setParallelism(1);

        // 配置kafka
        String topic = "topic001";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFAKA_BOOTSTRAP_SERVERS);
        properties.put("group.id", "consumer-group");
        properties.setProperty("auto.offset.reset", "earliest");

        // flink 添加 kafka数据源
        return env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
    }

    public static void execute(String jobName) throws Exception {
        env.execute(jobName);
    }
}
