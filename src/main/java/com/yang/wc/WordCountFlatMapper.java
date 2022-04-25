package com.yang.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zhangyang03
 * @Description
 * @create 2022-03-25 21:31
 */
public class WordCountFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
        String[] words = s.split(" ");
        for (String str : words) {
            collector.collect(new Tuple2<>(str, 1));
        }
    }
}
