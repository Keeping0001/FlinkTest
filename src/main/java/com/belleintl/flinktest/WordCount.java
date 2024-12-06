package com.belleintl.flinktest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理 wordcount
 *
 * @Author: zhipengl01
 * @Date: 2021-09-03
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.准备数据, 从文件中读取
        String filePath = "D:\\Doc\\Test\\word.txt";
        DataSet<String> inputDateSet = env.readTextFile(filePath);

        //3.处理数据
        DataSet<Tuple2<String, Integer>> outputDateSet = inputDateSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1);

        outputDateSet.print();
    }


    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}