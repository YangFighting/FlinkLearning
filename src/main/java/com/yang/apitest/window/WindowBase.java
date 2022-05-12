package com.yang.apitest.window;

import com.yang.apitest.pojo.Topic001;
import com.yang.utils.InputDataStreamUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
        KeyedStream<Topic001, Integer> keyByDataStream = dataStream.keyBy(Topic001::getId);

        // TimeWindow 滚动窗口
        DataStream<Topic001> minByScrollWindowStream = keyByDataStream.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .minBy("num");
        minByScrollWindowStream.print(" Scroll ");

        // TimeWindow 滑动窗口
        DataStream<Topic001> minBySlidingWindowStream = keyByDataStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)))
                .minBy("num");
        minBySlidingWindowStream.print(" Sliding ");

        // CountWindow  滚动窗口
        DataStream<Topic001> minByScrollCount = keyByDataStream.countWindow(5).minBy("num");
        minByScrollCount.print("Scroll countWindow ");

        // CountWindow  滑动窗口
        DataStream<Topic001> minBySlidingCount = keyByDataStream.countWindow(10 ,2).minBy("num");
        minBySlidingCount.print("Sliding countWindow ");

        InputDataStreamUtil.execute("WindowBase");
    }


}
