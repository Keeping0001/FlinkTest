package com.belleintl.flinktest;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StreamWordCount
 * @Description: 流处理 wordcount
 * @Author: zhipengl01
 * @Date: 2021/9/3
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度, 默认值=计算机cpu核数
        senv.setMaxParallelism(32);

        //3.准备数据, 从文件中读取数据
        String filePath = "D:\\Doc\\Test\\word.txt";
        DataStream<String> inputDateStream = senv.readTextFile(filePath);

        //4.处理数据
        DataStream<Tuple2<String, Integer>> resultDateStream = inputDateStream.flatMap(new MyFlatMapper()).keyBy(value -> value.f0).sum(1);

        resultDateStream.print();

        //执行任务
        senv.execute();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : s.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

    public static class MyReducer implements ReduceFunction<Integer> {

        @Override
        public Integer reduce(Integer value1, Integer value2) throws Exception {
            return value1 + value2;
        }
    }
}
