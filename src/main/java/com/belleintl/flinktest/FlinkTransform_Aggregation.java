package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: FlinkTransform_RollingAggregation
 * @Description: Flink聚合操作算子: keyBy,RollingAggregation,reduce
 * @Author: zhipengl01
 * @Date: 2021/9/16
 */
public class FlinkTransform_Aggregation {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(1);

        //准备数据
        DataStream<String> dataStream = senv.readTextFile("D:\\Doc\\Test\\sensor.txt");

        //2.处理数据
        DataStream<SensorReading> mapStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] value = line.split(",");
                return new SensorReading(value[0], new Long(value[1]), new Double(value[2]));
            }
        });

        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] value = line.split(",");
            return new SensorReading(value[0], Long.valueOf(value[1]), Double.valueOf(value[2]));
        });
        
        //先分组再聚合
        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(value -> value.getId());
//        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(SensorReading::getId);
        DataStream<SensorReading> resultStream = keyedStream.max("temperature");

        KeyedStream<SensorReading, String> keyedStream1 = sensorStream.keyBy(SensorReading::getId);
        SingleOutputStreamOperator<SensorReading> resultStream1 = keyedStream1.maxBy("temperature");

        DataStream<SensorReading> resultStream2 = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading curSensorReading, SensorReading newSensorReading) throws Exception {
                return new SensorReading(curSensorReading.getId(), curSensorReading.getTimestamp(), Math.max(curSensorReading.getTemperature(), newSensorReading.getTemperature()));
            }
        });

        DataStream<SensorReading> resultStream3 = keyedStream.reduce(
                (curSensor, newSensor) -> new SensorReading(curSensor.getId(), newSensor.getTimestamp(), Math.max(curSensor.getTemperature(), newSensor.getTemperature()))
        );

        resultStream.print("result");
        resultStream1.print("result1");
        resultStream2.print("result2");
        resultStream3.print("result3");

        senv.execute();
    }
}
