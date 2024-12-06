package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName: FlinkOperateState
 * @Description: Flink算子状态
 * @Author: zhipengl01
 * @Date: 2021/9/30
 */
public class FlinkOperateState {
    public static void main(String[] args) throws Exception {
        //1.准备执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(1);

        //设置数据源
        DataStream<String> dataSource = senv.socketTextStream("localhost", 7777);

        //换成SensorReading类型
        DataStream<SensorReading> dataStream = dataSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] feilds = value.split(",");
                return new SensorReading(feilds[0], Long.valueOf(feilds[1]), new Double(feilds[2]));
            }
        });

        //定义一个有状态的map操作,统计当前分区数据个数
        SingleOutputStreamOperator<Integer> result = dataStream.map(new MyCountMapper());

        result.print();

        senv.execute();
    }

    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        //定义一个本地变量,作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            return count++;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            //对状态做快照
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            // 发生故障时调用该方法 恢复快照
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
