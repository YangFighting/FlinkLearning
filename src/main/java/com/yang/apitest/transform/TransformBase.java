package com.yang.apitest.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;

/**
 * @author zhangyang03
 * @Description 基本转换算子
 * @create 2022-04-26 17:12
 */
public class TransformBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取数据
        Properties prop = new Properties();
        prop.put("inputFilePath","E:\\project_java\\FlinkLearning\\src\\main\\resources\\topic001.txt");
        String inputFilePath = prop.getProperty("inputFilePath");
        DataStream<String> dataStream = env.readTextFile(inputFilePath);

        // 1. map 计算json字符串的个数
        DataStream<Integer> mapDateStream = dataStream.map(s -> JSON.parseObject(s).size());

        // 2. flatMap 将 json字符串 转换成 key_value
        DataStream<String> flatMapDateStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                    collector.collect(entry.getKey() + "_" + entry.getValue().toString());
                }
            }
        });

        // 3. filter 筛选 id 为 2 的数据
        DataStream<String> filterDataStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return "2".equals(jsonObject.get("id").toString());
            }
        });

        mapDateStream.print("mapDateStream");
        flatMapDateStream.print("flatMapDateStream");
        filterDataStream.print("filter--DataStream");
        env.execute("TransformBase");
    }
}
