package com.yang.apitest.transform;

import com.yang.apitest.pojo.Topic001;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author zhangyang03
 * @Description 测试 富函数
 * @create 2022-05-09 14:28
 */
public class TransformRichFunction {
    private static final Logger logger = LoggerFactory.getLogger(TransformRichFunction.class);

    public static void main(String[] args) throws Exception {
        DataStream<Topic001> dataStream = TransformPublic.getTopoc001DataStream();
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());
        resultStream.print();
        TransformPublic.execute("TransformRichFunction");
    }

    // 实现自定义富函数类（RichMapFunction是一个抽象类）
    public static class MyMapper extends RichMapFunction<Topic001,Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> map(Topic001 topic001) {
            return new Tuple2<>(topic001.getId().toString(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 初始化工作，一般是定义状态，或者建立数据库连接
            logger.warn("---- open ----");
        }

        @Override
        public void close() throws Exception {
            super.close();

            // 一般是关闭连接和清空状态的收尾操作
            logger.warn("---- close ----");
        }
    }
}


