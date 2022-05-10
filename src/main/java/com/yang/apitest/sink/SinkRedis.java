package com.yang.apitest.sink;

import com.yang.utils.InputDataStreamUtil;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author zhangyang03
 * @Description 将Redis当作sink的输出对象
 * @create 2022-05-10 11:16
 */
public class SinkRedis {
    public static void main(String[] args) {
        // flink 添加 kafka数据源
        DataStream<String> inputDataStream = InputDataStreamUtil.getDataStreamFromKafka();
    }
}
