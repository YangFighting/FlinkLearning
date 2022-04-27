package com.yang.apitest.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import com.yang.utils.JsonUtil;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;

/**
 * @author zhangyang03
 * @Description 聚合操作算子 Reduce
 * @create 2022-04-27 11:33
 */
public class TransformReduce {
    public static void main(String[] args) throws Exception {
        DataStream<String> dataStream = TransformPublic.getDataStreamFroText();
        DataStream<Topic001> mapDataStream = dataStream.filter(JsonUtil::isJson).map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()), new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()), Integer.valueOf(jsonObject.get("num").toString()));
        });

        DataStream<Topic001> reduceDataStream = mapDataStream.keyBy(Topic001::getId).reduce(new ReduceFunction<Topic001>() {
            @Override
            public Topic001 reduce(Topic001 topic001, Topic001 t1) throws Exception {
                //   reduce会保存之前计算的结果，然后和新的数据进行累加，所以每次输出的都是历史所有的数据的总和
                //   第一个参数t是保存的历史数据，t1是最新的数据
                return new Topic001(t1.getId(), t1.getTime(), Math.max(topic001.getNum(), t1.getNum()));
            }
        });
        reduceDataStream.print("reduceDataStream");

        TransformPublic.execute("TransformReduce");
    }
}
