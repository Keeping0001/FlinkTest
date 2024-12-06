package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName: FlinkSource_UDF
 * @Description: Flink数据源_自定义
 * @Author: zhipengl01
 * @Date: 2021/9/3
 */
public class FlinkSource_UDF {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(4);

        //2.设置自定义数据源环境
        DataStream<SensorReading> dataStream = senv.addSource(new MySensorSource());

        dataStream.print();

        senv.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        //标志位,控制数据产生
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //定义一个随机数产生
            Random random = new Random();

            //设置10个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i+1), 60 + random.nextGaussian() * 20);
            }

            while(running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    //在当前温度基础上随机波动
                    double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
//                    System.out.println(sensorTempMap);
                }

                //控制输出频率
                Thread.sleep(2000L);
            }

        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
