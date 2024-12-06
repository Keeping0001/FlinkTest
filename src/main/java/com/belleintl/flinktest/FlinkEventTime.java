package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName: FlinkEeventTime
 * @Description: Flink事件时间
 * @Author: zhipengl01
 * @Date: 2021/10/23
 */
public class FlinkEventTime {
    public static void main(String[] args) throws Exception {
        //1.准备执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);

        //新版Flink1.12已经默认为事件时间 所以可以不用设置
//        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印的周期时间
        senv.getConfig().setAutoWatermarkInterval(200L);

        //2.接入数据源
        DataStream<String> dataSource = senv.socketTextStream("", 7777);

        //3.处理数据, 将数据转换成Sensoring类型, 并分配时间戳和watermark
        DataStream<SensorReading> dataStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });

        DataStream<SensorReading> waterMarkDataStream = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(20)).withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
            @Override
            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                return 0;
            }
        }));

        DataStream<SensorReading> newWaterMarkDataStream = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner((SerializableTimestampAssigner<SensorReading>) (element, recordTimestamp) -> element.getTimestamp() * 1000L));

        //定义侧边输出流标签
        OutputTag<SensorReading> outPutTag = new OutputTag<>("later");


        //基于事件时间的开窗聚合,统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempDataStream = waterMarkDataStream
//                .keyBy("id")
//                .keyBy(data -> data.getId())
                .keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15))) //flink1.12新版本的时间窗口  15s的滑动窗口
                .allowedLateness(Time.minutes(1)) //允许迟到一分钟的数据
                .sideOutputLateData(outPutTag) //侧边输出流
                .minBy("temperature");
//                .timeWindow(Time.seconds(15)) //15s的一个时间窗口

        minTempDataStream.print("minTemp");
        minTempDataStream.getSideOutput(outPutTag).print("later"); //获取侧边输出流并打印出来

        //执行
        senv.execute();
    }
}
