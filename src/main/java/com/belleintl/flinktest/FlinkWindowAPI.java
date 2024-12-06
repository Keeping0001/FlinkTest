package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: FlinkWindowAPI
 * @Description: Flink窗口APi: 增量聚合函数
 * @Author: zhipengl01
 * @Date: 2021/9/30
 */
public class FlinkWindowAPI {
    public static void main(String[] args) throws Exception{
        //1.准备执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);

        //2.准备数据
        DataStream<String> dataSource = senv.readTextFile("D:\\Doc\\Test\\sensor.txt");



        //3.数据处理
        DataStream<SensorReading> dataStream = dataSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] contents = value.split(",");
                return new SensorReading(contents[0], Long.valueOf(contents[1]), new Double(contents[2]));
            }
        });

        DataStream<Tuple2<String, Double>> result = dataStream.map(new MyMapper())
                .keyBy(data -> data.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
                .minBy(1);

        result.print();

        //执行代码
        senv.execute();
    }

    public static class MyMapper implements MapFunction<SensorReading, Tuple2<String, Double>>{

        @Override
        public Tuple2<String, Double> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), Double.valueOf(value.getTemperature()));
        }
    }
}
