package com.yang.apitest.state.operator_state;

import com.yang.apitest.pojo.Event;
import com.yang.apitest.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangyang03
 * @Description 列表状态的平均分割重组（event-split redistribution） 自定义的 SinkFunction 会在进行数据缓存，然后统一发送到下游。这个例子演示了
 * @create 2022-06-07 19:58
 */
public class BufferingSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp));

        stream.print("input");
        //批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private final int threshold;
        private final List<Event> bufferedElements;
        // 定义算子状态
        private transient ListState<Event> checkpointedState;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("=====输出完毕======");
                bufferedElements.clear();
            }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointedState.clear();

            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Types.POJO(Event.class));
            checkpointedState =
                    functionInitializationContext.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
            if (functionInitializationContext.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }

        }
    }
}
