package com.yang.apitest.state.keyed_state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhangyang03
 * @Description 使用列表状态（ListState），模拟 两条流的全量 Join
 * @create 2022-05-31 20:29
 */
public class TwoStreamFullJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env
                .fromElements(
                        Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 2000L),
                        Tuple3.of("c", "stream-1", 3000L),
                        Tuple3.of("a", "stream-1", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>)
                                (stringStringLongTuple3, l) -> stringStringLongTuple3.f2));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env
                .fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L),
                        Tuple3.of("a", "stream-2", 5000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>)
                                (stringStringLongTuple3, l) -> stringStringLongTuple3.f2));

        // 自定义列表状态进行全外联结
        SingleOutputStreamOperator<String> resultStream = stream1.keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<>() {

                    // 定义列表状态，保持两条流中已经到达的所有数据
                    private ListState<Tuple3<String, String, Long>>
                            stream1ListState;
                    private ListState<Tuple3<String, String, Long>>
                            stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        stream1ListState = getRuntimeContext().
                                getListState(new ListStateDescriptor<>
                                        ("stream1-list", Types.TUPLE(Types.STRING, Types.STRING))
                                );
                        stream2ListState = getRuntimeContext().
                                getListState(new ListStateDescriptor<>
                                        ("stream2-list", Types.TUPLE(Types.STRING, Types.STRING))
                                );

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left,
                                                Context context, Collector<String> collector) throws Exception {
                        stream1ListState.add(left);

                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            collector.collect(left + " => " + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right,
                                                Context context, Collector<String> collector) throws Exception {
                        stream2ListState.add(right);
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            collector.collect(left + " => " + right);
                        }
                    }
                });
        resultStream.print();
        env.execute();
    }
}
