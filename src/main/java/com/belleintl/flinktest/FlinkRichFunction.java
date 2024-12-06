package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

/**
 * @ClassName: FlinkRichFunction
 * @Description: Flink富函数
 * @Author: zhipengl01
 * @Date: 2021/9/24
 */
public class FlinkRichFunction {
    public static void main(String[] args) throws Exception{
        //1.准备执行环境/
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);

        //2.准备数据
        DataStream<String> dataSource = senv.readTextFile("D:\\Doc\\Test\\sensor.txt");

        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });


        DataStream<Tuple2<String, Integer>> idLengthResult = dataStream.map(new MapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getId().length());
            }
        });

        DataStream<Tuple2<String, Long>> idAndTimeResult = dataStream.map(new MyMapper());

        DataStream<Tuple2<String, Double>> idAndTempResult = dataStream.map(new MyRichMapper());

        idLengthResult.print("idLengthResult");
        idAndTimeResult.print("idAndTimeResult");
        idAndTempResult.print("idAndTempResult");

        //执行代码
        senv.execute();

    }

    public static class MyMapper implements MapFunction<SensorReading, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getTimestamp());
        }
    }

    public static class MyRichMapper extends RichMapFunction<SensorReading, Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> map(SensorReading value) throws Exception {
            //RichFunction可以获取State状态
//            getRuntimeContext().getState();
            return new Tuple2<>(value.getId(), value.getTemperature());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("this is opening!");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("this is closing!");
        }
    }
}
