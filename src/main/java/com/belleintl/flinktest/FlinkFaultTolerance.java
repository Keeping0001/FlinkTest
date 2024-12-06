package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName: FlinkFaultTolerance
 * @Description: Flink容错机制
 * @Author: zhipengl01
 * @Date: 2021/10/23
 */
public class FlinkFaultTolerance {
    public static void main(String[] args) throws Exception{
        //1.准备环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);
        //设置周期性水印时间
        senv.getConfig().setAutoWatermarkInterval(200L);

        // 状态后端配置
        senv.setStateBackend(new FsStateBackend("checkpointDataUri")); //文件系统的状态后端, 默认状态后端
//        senv.setStateBackend(new MemoryStateBackend()); //内存级状态后端, 一般用在测试中

        // 设置checkpoint时间 设定jobmanager每隔300ms进行一次checkpoint
        senv.enableCheckpointing(300L);

        // 高级选项

        // 设定精确一次模式
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint的处理超时时间
        senv.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 最大允许处理几个checkpoint
        senv.getCheckpointConfig().setMaxConcurrentCheckpoints(4);
        // 与上面setMaxConcurrentCheckpoints(2) 冲突，这个时间间隔是 当前checkpoint的处理完成时间与接收最新一个checkpoint之间的时间间隔
        senv.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        // 如果同时开启了savepoint且有更新的备份，是否倾向于使用更老的自动备份checkpoint来恢复，默认false
        senv.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 最多能容忍几次checkpoint处理失败（默认0，即checkpoint处理失败，就当作程序执行异常）
        senv.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 重启策略配置
        //固定延迟重启 (最多尝试3次, 每次间隔10s)
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        //失败率重启 (在10分钟内最多尝试3次, 每次至少间隔1分钟)
        senv.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        //2.读取数据
        DataStream<String> dataSource = senv.socketTextStream("", 7777);

        //数据转换成SensorReading
        DataStream<SensorReading> mapDataStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });

        //分配事件时间和waterMark
        SingleOutputStreamOperator<SensorReading> waterMarkDataStream = mapDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(200L))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        //定义侧边输出标签
        OutputTag<SensorReading> outputTag = new OutputTag<>("later");

        //3.数据的转换
        SingleOutputStreamOperator<SensorReading> resultDataStream = mapDataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(20)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        //4.开启执行
        senv.execute();
    }
}
