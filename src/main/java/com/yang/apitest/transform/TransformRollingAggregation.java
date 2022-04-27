package com.yang.apitest.transform;

import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;


/**
 * @author zhangyang03
 * @Description 滚动聚合，测试
 * @create 2022-04-26 19:30
 */
public class TransformRollingAggregation {

    public static void main(String[] args) throws Exception {
        DataStream<Topic001> mapDataStream = TransformPublic.getTopoc001DataStream();
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
