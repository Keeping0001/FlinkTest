package com.belleintl.flinktest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: FlinkTransform_Base
 * @Description: Flink基本转换算子: map,flatMap,filter
 * @Author: zhipengl01
 * @Date: 2021/9/16
 */
public class FlinkTransform_Base {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);

        //2.数据准备
        DataStream<String> dataStream = senv.readTextFile("D:\\Doc\\Test\\word.txt");

        //数据处理
        //1.map, String=>字符串长度
        DataStream<Integer> mapStream = dataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        //2.flatMap,按照空格分割字符串
        DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(" ");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        //3.filter,筛选flink字符串
        DataStream<String> filterStream = flatMapStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
//                return value == "flink";
                return value.equals("flink");
            }
        });


        //输出结果
        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        senv.execute();
    }
}
