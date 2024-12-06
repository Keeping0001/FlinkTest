package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: FlinkKeyedProcessFunction
 * @Description: FlinkKeyedProcessFunction
 * @Author: zhipengl01
 * @Date: 2021/10/24
 */
public class FlinkKeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);
        //设置周期生成水印时间
        senv.getConfig().setAutoWatermarkInterval(2000L);
        //设置状态后端
        senv.setStateBackend(new FsStateBackend("checkpoingUri"));

        //2.获取数据
        DataStream<String> dataSource = senv.socketTextStream("", 7777);

        //将数据转换成SensorReading
        DataStream<SensorReading> dataStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), new Double(fields[2]));
        });

        //测试KeyedProcessFunction
        dataStream.keyBy(SensorReading::getId)
                .process(new MyProcess())
                .print();


        //检测一段时间内温度连续上升
        dataStream.keyBy(SensorReading::getId)
                .process(new MyTempConsIncreWarning(10))
                .print();


        //定义一个侧输出流标签
        OutputTag<SensorReading> lowTemp = new OutputTag<>("low_temp");


        //定义侧边输出流实线分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                // 判断温度, 大于30度, 温度输出到主流中, 小于低温就输出到侧输出流中
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(lowTemp, value);
                }
            }
        });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTemp).print("low-temp");

        //启动执行
        senv.execute();

    }

    public static class MyProcess extends KeyedProcessFunction<String, SensorReading, Integer> {

        ValueState<Long> tsTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-time", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            //context : 访问元素的时间戳、元素key、TimeService实践服务，以及还可以将结果输出到别的流 side output。
            ctx.timestamp();  //获取当前时间戳
            ctx.getCurrentKey();  //获取当前Key
            ctx.timerService().currentWatermark();  //获取当前的水印|事件时间
            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);  //注册事件时间的定时器
            ctx.timerService().deleteEventTimeTimer((value.getTimestamp()+100) * 1000L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");

            ctx.getCurrentKey();
            ctx.timeDomain(); //时间域
        }

        @Override
        public void close() throws Exception {
            tsTimeState.clear();
        }
    }

    public static class MyTempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {

        //定义一个时间间隔
        private final Integer interval;

        public MyTempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        //定义状态,保存上一次记录的温度,定时器时间戳
        ValueState<Double> lastTemperature;
        ValueState<Long> timeTsState;


        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timeTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-ts-state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //取出状态
            Double curTemp = value.getTemperature();  //当前温度值
            //上一次温度, 如果没有则设定为当前温度
            Double lastTemp = lastTemperature.value() == null ? curTemp : lastTemperature.value();
            //计时器状态值
            Long timerTimestamp = timeTsState.value();

            //如果温度上升并且没有定时器,注册10秒后的定时器, 开始等待
            if(curTemp > lastTemp && null == timerTimestamp) {
//                long warningTimestamp = ctx.timerService().currentProcessingTime() + interval;
//                ctx.timerService().registerProcessingTimeTimer(warningTimestamp);  //注册一个处理时间的定时器
                long warningEventTimestamp = ctx.timerService().currentWatermark() + interval;
                ctx.timerService().registerEventTimeTimer(warningEventTimestamp);  //注册一个事件时间的定时器

//                timeTsState.update(warningTimestamp);
                timeTsState.update(warningEventTimestamp);
            }
            //如果温度下降, 删除定时器
            else if(curTemp < lastTemp && timerTimestamp != null) {
                ctx.timerService().deleteEventTimeTimer(timerTimestamp);
                timeTsState.clear();
            }

            //更新保存的温度值
            lastTemperature.update(curTemp);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "s上升");
            timeTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTemperature.clear();
        }
    }

}
