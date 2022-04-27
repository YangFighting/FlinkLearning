package com.yang.apitest.transform;

import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple2;
import scala.Tuple3;

/**
 * @author zhangyang03
 * @Description 多流转换算子  connect, CoMap(map)  union
 * @create 2022-04-27 14:32
 */
public class TransformMultipleStreams {

    public static void main(String[] args) throws Exception {
        DataStream<Topic001> dataStream = TransformPublic.getTopoc001DataStream();

        // 1. 根据 filter 分流，按照num 为 50 为界分为两条流
        DataStream<Topic001> highNumDataStream = dataStream.filter(topic001 -> (topic001.getNum() >= 50));
        DataStream<Topic001> lowNumDataStream = dataStream.filter(topic001 -> (topic001.getNum() < 50));

        // 2. 转换 流
        DataStream<Tuple2<Integer, String>> highLogDataStream = highNumDataStream.map(new MapFunction<Topic001, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(Topic001 topic001) throws Exception {
                return new Tuple2<>(topic001.getId(), "highNum");
            }
        });

        // 3. connect 合流，构造 ConnectedStreams
        ConnectedStreams<Topic001, Tuple2<Integer, String>> connectedStreams = lowNumDataStream.connect(highLogDataStream);

        // 4 CoMap 合流 ，构造 DataStream
        DataStream<Object> coMapDataStream = connectedStreams.map(new CoMapFunction<Topic001, Tuple2<Integer, String>, Object>() {
            @Override
            public Object map1(Topic001 topic001) throws Exception {
                return new Tuple3<>(topic001.getId(), topic001.getNum(), "topic001 data");
            }

            @Override
            public Object map2(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2;
            }
        });


        coMapDataStream.print("coMapDataStream");

        DataStream<Topic001> unionDataStream = highNumDataStream.union(lowNumDataStream, dataStream);
        unionDataStream.print("unionDataStream");

        TransformPublic.execute("TransformMultipleStreams");
    }


}
