package com.yang.apitest.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import java.text.SimpleDateFormat;

/**
 * @author zhangyang03
 * @Description 滚动聚合，测试
 * @create 2022-04-26 19:30
 */
public class TransformRollingAggregation {

    public static void main(String[] args) throws Exception {
        DataStream<String> dataStream = TransformPublic.getDataStreamFroText();

        DataStream<Topic001> mapDataStream = dataStream.map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(jsonObject.get("time").toString()), Integer.valueOf(jsonObject.get("num").toString()));
        });
        // 先分组再聚合
        KeyedStream<Topic001, Integer> keyedDataStream = mapDataStream.keyBy(new KeySelector<Topic001, Integer>() {
            @Override
            public Integer getKey(Topic001 topic001) throws Exception {
                return topic001.getId();
            }
        });
        // 在keyBy中，flink支持的需要是POJO类，或者tuple、样例类
        DataStream<Topic001> maxByStream = keyedDataStream.maxBy("num");
        maxByStream.print("maxByStream");
        TransformPublic.execute("TransformRollingAggregation");
    }
}
