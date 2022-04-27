package com.yang.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author zhangyang03
 * @Description 转换算子 公共方法
 * @create 2022-04-27 11:10
 */
public class TransformPublic {
    private static StreamExecutionEnvironment env;

    private TransformPublic() {
    }

    public static DataStream<String> getDataStreamFroText() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取数据
        Properties prop = new Properties();
        prop.put("inputFilePath", "E:\\project_java\\FlinkLearning\\src\\main\\resources\\topic001.txt");
        String inputFilePath = prop.getProperty("inputFilePath");
        return env.readTextFile(inputFilePath);
    }

    public static void execute(String jobName) throws Exception {
        env.execute(jobName);
    }
}
