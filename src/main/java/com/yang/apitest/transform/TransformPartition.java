package com.yang.apitest.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yang.apitest.pojo.Topic001;
import com.yang.utils.JsonUtil;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.text.SimpleDateFormat;

/**
 * @author zhangyang03
 * @Description 数据重分区操作
 * @create 2022-05-10 9:51
 */
public class TransformPartition {
    public static void main(String[] args) throws Exception {

        DataStream<String> inputDataStream = TransformPublic.getDataStreamFromText();
        DataStream<Topic001> dataStream = inputDataStream.filter(JsonUtil::isJson).map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            return new Topic001(Integer.valueOf(jsonObject.get("id").toString()), new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").parse(jsonObject.get("time").toString()), Integer.valueOf(jsonObject.get("num").toString()));
        });

        // 1. SingleOutputStreamOperator多并行度默认就rebalance,轮询方式分配
        inputDataStream.print("input");

        DataStream<String> shuffleStream = inputDataStream.shuffle();
        shuffleStream.print("shuffle");

        // 2. keyBy (Hash，然后取模)
        dataStream.keyBy(Topic001::getId).print("keyBy");

        // 3. global (直接发送给第一个分区，少数特殊情况才用)
        dataStream.global().print("global");

        TransformPublic.execute("TransformPartition");
    }
}
