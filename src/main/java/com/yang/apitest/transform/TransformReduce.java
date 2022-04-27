package com.yang.apitest.transform;

import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * @author zhangyang03
 * @Description 聚合操作算子 Reduce
 * @create 2022-04-27 11:33
 */
public class TransformReduce {
    public static void main(String[] args) throws Exception {
        DataStream<Topic001> mapDataStream = TransformPublic.getTopoc001DataStream();

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
