package com.belleintl.flinktest;

import akka.japi.tuple.Tuple3;
import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @ClassName: FlinkKeyedState
 * @Description: Flink键控状态
 * @Author: zhipengl01
 * @Date: 2021/9/30
 */
public class FlinkKeyedState {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        senv.setParallelism(1);

        //设置数据源
        DataStream<String> dataSource = senv.socketTextStream("localhost", 7777);

        //换成SensorReading类型
        DataStream<SensorReading> dataStream = dataSource.map(line -> {
            String[] feilds = line.split(",");
            return new SensorReading(feilds[0], Long.valueOf(feilds[1]), new Double(feilds[2]));
        });

        //使用自定义map方法,里面使用 我们自定义的keyedState
        SingleOutputStreamOperator<Integer> result = dataStream.keyBy(SensorReading::getId)
                .map(new MyMapper());

        //温度检测报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> tempResult = dataStream.keyBy(SensorReading::getId)
                .flatMap(new MyFlatMapper(10.0));

        result.print();

        senv.execute();
    }

    //自定义map富函数,测试 监控状态
    public static class MyMapper extends RichMapFunction<SensorReading,Integer> {

        //值状态:将状态表示为单个的值
        private ValueState<Integer> valueState;

        //列表状态:将状态表示为一组数据的列表
        private ListState<String> listState;

        //映射状态:将状态表示为一组key-value对
        private Map<String, Double> mapState;

        //聚合状态:将状态表示为一个用于聚合操作的列表
        private ReducingState<SensorReading> myReducerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));

            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            mapState = (Map<String, Double>) getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
//            myReducerState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>())
        }

        @Override
        public Integer map(SensorReading value) throws Exception {

            //其他状态API调用
            for (String str : listState.get()) {
                System.out.println(str);
            }
            listState.add("hello");

            mapState.get("1");
            mapState.put("2",12.3);
            mapState.remove("2");

            mapState.clear();

            Integer count = valueState.value();
            //第一次获取为null,需要判断一下
            count = count==null?0:count;
            ++count;
            valueState.update(count);

            return count;
        }

        @Override
        public void close() throws Exception {
            System.out.println("this is closing!");
            super.close();
        }
    }

    //如果传感器温度前后相差找过指定温度,就报警
    public static class MyFlatMapper extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>> {
        //报警的温差阈值
        private final Double threshold;

        //记录上一次的温度
        ValueState<Double> lastTemperature;

        public MyFlatMapper(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //从运行时上下文中获取keyedState
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTemperature.value();
            Double curTemp = value.getTemperature();

            // 如果不为空, 就判断一下是否温差超过阈值, 超过则报警
            if (lastTemp != null) {
                if (Math.abs(curTemp-lastTemp) >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, curTemp)); //输出值
                }
            }

            // 更新保存上一次温度
            lastTemperature.update(curTemp);
        }

        @Override
        public void close() throws Exception {
            //手动释放资源
            lastTemperature.clear();
        }


    }
}
