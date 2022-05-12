package com.yang.apitest.window;

import com.yang.apitest.pojo.Topic001;
import com.yang.utils.InputDataStreamUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhangyang03
 * @Description 时间串口
 * @create 2022-05-12 10:05
 */
public class WindowBase {
    public static void main(String[] args) throws Exception {
        DataStream<String> inputDataStream = InputDataStreamUtil.getDataStreamFromSocket("localhost", 7777);
        DataStream<Topic001> dataStream = InputDataStreamUtil.getTopoc001FromStringDataStream(inputDataStream);

        // 滚动窗口
        SingleOutputStreamOperator<Topic001> minByWindowStream = dataStream.keyBy(Topic001::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .minBy("num");

        minByWindowStream.print("minByWindowStream");


        InputDataStreamUtil.execute("WindowBase");
    }


}
