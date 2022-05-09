package com.yang.apitest.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import com.yang.utils.JsonUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
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

    public static DataStream<String> getDataStreamFromText() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 从文件中读取数据
        Properties prop = new Properties();
        prop.put("inputFilePath", "E:\\project_java\\FlinkLearning\\src\\main\\resources\\topic001.txt");
        String inputFilePath = prop.getProperty("inputFilePath");
        return env.readTextFile(inputFilePath);
    }

    public static DataStream<Topic001> getTopoc001DataStream(){
        DataStream<String> dataStream = TransformPublic.getDataStreamFromText();
        return dataStream.filter(JsonUtil::isJson).map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()), new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()), Integer.valueOf(jsonObject.get("num").toString()));
        });
    }

    public static void execute(String jobName) throws Exception {
        env.execute(jobName);
    }
}
