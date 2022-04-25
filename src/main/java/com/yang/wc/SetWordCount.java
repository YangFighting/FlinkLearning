package com.yang.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Properties;

/**
 * @author zhangyang03
 * @Description 批处理 wordcount
 * @create 2022-03-25 21:25
 */
public class SetWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        Properties prop = new Properties();
        prop.put("inputFilePath","E:\\project_java\\FlinkLearning\\src\\main\\resources\\hello.txt");
        String inputFilePath = prop.getProperty("inputFilePath");

        // 对数据集批处理，按照空格 分词统计
        DataSet<String> inputDataSet = environment.readTextFile(inputFilePath);

        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new WordCountFlatMapper())
                .groupBy(0)
                .sum(1);

        resultSet.print();

    }

}
