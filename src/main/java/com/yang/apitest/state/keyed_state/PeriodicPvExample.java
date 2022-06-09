package com.yang.apitest.state.keyed_state;

import com.yang.apitest.pojo.Event;
import com.yang.apitest.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhangyang03
 * @Description 使用 值状态（ValueState） ，定时统计用户总数（不分窗口）
 * @create 2022-05-25 20:28
 */
public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 统计每个用户的pv,隔一段时间输出 一次结果
        stream.keyBy(data -> data.user).process(new PeriodicPvResult()).print("PeriodicPvResult");
        env.execute();
    }

    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        // 定义两个状态，保存当前 pv 值，以及定时器时间戳
        transient ValueState<Long>  countState;
        transient ValueState<Long> timerTsState;

        @Override
        public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
            // 更新  count 值
            Long count = countState.value();
            if (count == null) {
                countState.update(1L);
            } else {
                countState.update(count + 1);
            }

            // 判断定时器时间戳状态有没有值，如果没有值，则注册定时器
            if (timerTsState.value() == null) {
                context.timerService().registerEventTimeTimer(event.timestamp + 10 * 1000L);
                // 更新 定时器时间戳状态
                timerTsState.update(event.timestamp + 10 * 1000L);
            }
            // 收集 当前用户 和 总数
            // 与 onTimer 的 collector 对比
            collector.collect(context.getCurrentKey() + " -- " + countState.value());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取 定义的键控状态
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，搜集 当前用户以及 总数
            out.collect(ctx.getCurrentKey() + " pv : " + countState.value());
            // 清空状态
            timerTsState.clear();

            // 注册定时器，用于下一次统计
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerTsState.update(timestamp + 10 * 1000L);

            // 因为 是累计统计，这里 不需要清空 countState
//            countState.clear();


        }
    }


}
