package com.yang.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author zhangyang03
 * @Description 流处理 wordcount
 * @create 2022-04-21 20:24
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，默认值 = 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        environment.setParallelism(1);
        // 从文件中读取数据
//        Properties prop = new Properties();
//        prop.put("inputFilePath","E:\\project_java\\FlinkLearning\\src\\main\\resources\\hello.txt");
//        String inputFilePath = prop.getProperty("inputFilePath");
//        DataStream<String> dataStream = environment.readTextFile(inputFilePath);

        // 从 socket 读取数据
        DataStream<String> dataStream = environment.socketTextStream("10.91.3.37", 7777);
        DataStream<Tuple2<String, Integer>> outputStream = dataStream.flatMap(new WordCountFlatMapper()).keyBy(item -> item.f0).sum(1);
        outputStream.print();
        environment.execute();

    }
}
