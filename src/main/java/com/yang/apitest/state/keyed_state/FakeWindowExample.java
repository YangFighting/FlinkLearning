package com.yang.apitest.state.keyed_state;

import com.yang.apitest.pojo.Event;
import com.yang.apitest.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author zhangyang03
 * @Description 使用 KeyedProcessFunction 模拟滚动窗口，每10秒都会打印各个网站页面的访问数
 * @create 2022-06-01 19:52
 */
public class FakeWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp));

        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))      // 10秒的窗口长度
                .print();


        env.execute();
    }


    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        private final Long windowSize;
        // 声明状态，用 map 保存 pv 值（窗口 start， count）, map的key 表示哪一个窗口
        // 这里之所以用 map 是因为，存在延迟到达的数据
        MapState<Long, Long> windowPvMapState;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowPvMapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<Long, Long>("window-pv", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, Context context, Collector<String> collector) throws Exception {

            // 每来一条数据，就根据时间戳判断属于哪个窗口，窗口分配器的功能
            // timestamp / windowSize 是整数，表示是哪个窗口
            long windowStart = event.timestamp / windowSize * windowSize;

            long windowEnd = windowStart + windowSize;

            // 注册 end -1 的定时器，窗口触发计算，窗口触发时就是窗口关窗的时候
            context.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态中的 pv值（网站页面的访问数）
            if (windowPvMapState.contains(windowStart)) {
                Long pv = windowPvMapState.get(windowStart);
                windowPvMapState.put(windowStart, pv + 1);
            } else {
                windowPvMapState.put(windowStart, 1L);
            }
        }

        // 定时器触发，直接输出统计的 pv 结果
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;

            Long pv = windowPvMapState.get(windowStart);

            out.collect("url: " + ctx.getCurrentKey()
                    + " 访问量: " + pv
                    + " 窗 口 ： " + new Timestamp(windowStart) + " ~ " + new
                    Timestamp(windowEnd));

            // 模拟窗口的销毁，清除 map 中的 key
            windowPvMapState.remove(windowStart);
        }
    }
}
