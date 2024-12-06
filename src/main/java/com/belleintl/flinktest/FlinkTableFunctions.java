package com.belleintl.flinktest;


import akka.stream.impl.fusing.Collect;
import com.belleintl.finkbean.SensorReading;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


import java.util.Set;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName: FlinkTableFunctions
 * @Description: FlinkTable函数
 * @Author: zhipengl01
 * @Date: 2021/12/3
 */
public class FlinkTableFunctions {
    public static void main(String[] args) throws Exception{
        // 1.准备执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv);

        // 设置并行度
        senv.setParallelism(4);
        // 设置水印生成时间
        senv.getConfig().setAutoWatermarkInterval(200L);
        // 设置状态后端
//        senv.setStateBackend(new FsStateBackend(""));

        // 设置checkpoint周期时间
        senv.enableCheckpointing(100L);
        // 设置checkpoint模式: 精准一次
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 2.获取数据
        DataStreamSource<String> inputStream = senv.readTextFile("D:\\Doc\\Test\\sensor.txt");

        // 转换成POJO类
        DataStream<SensorReading> sensorReadingDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), Double.valueOf(fields[2]));
        });

        // 3.将流转换成表
//        Table sensorTable = tableEnv.fromDataStream(sensorReadingDataStream, "id, timestamp as ts, temperature");
        // 新版本推荐使用
        Table sensorTable = tableEnv.fromDataStream(sensorReadingDataStream, $("id"), $("timestamp").as("ts"), $("temperature"));


        // TODO: 4.自定义标量函数, 实现求id的hash值
        HashCode hashCode = new HashCode(23);
        // 注册UDF
//        tableEnv.registerFunction("hashCode", hashCode);
        // 新版本推荐使用
        tableEnv.createTemporarySystemFunction("hashCode", hashCode);

        // 4.1 table API
        Table resultTable = sensorTable.select($("id"), $("ts"), call("hashCode", $("id")));

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, temperature, hashCode(id) from sensor");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result_table");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("result_sql_table");

        // TODO: 5.自定义表函数
        Split split = new Split("_");
        // 注册udf函数
        tableEnv.createTemporarySystemFunction("split", split);

        // 5.1 table API
        Table resultTable_2 = sensorTable.joinLateral(call("split", $("id")))
                .select($("id"), $("ts"), $("word"), $("length"));

        Table resultTable_3 = sensorTable.leftOuterJoinLateral(call("split", $("id")).as("word", "length"))
                .select($("id"), $("ts"), $("word"), $("length"));


        // 5.2 SQL
//        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable_2 = tableEnv.sqlQuery("select id, ts, word, length from sensor, lateral table(split(id)) as splitid(word, length)");

        // 输出
        tableEnv.toAppendStream(resultTable_2, Row.class).print("result_table_2");
        tableEnv.toAppendStream(resultTable_3, Row.class).print("result_table_3");
        tableEnv.toAppendStream(resultSqlTable_2, Row.class).print("result_sql_table_2");

        // TODO: 6.自定义聚合函数, 求当前传感器的平均温度值
        AvgTemp avgTemp = new AvgTemp();
        AvgTemp_2 avgTemp_2 = new AvgTemp_2();
        // 注册udf函数
        tableEnv.createTemporarySystemFunction("avgTemp", avgTemp);
        tableEnv.createTemporarySystemFunction("avgTemp_2", avgTemp_2);

        // 6.1 table API
        Table resultTable_4 = sensorTable.groupBy($("id"))
                .aggregate(call("avgTemp", $("temperature")).as("temp_avg"))
                .select($("id"), $("temp_avg"));


        // 6.2 SQL
        Table resultSqlTable_3 = tableEnv.sqlQuery("select id, avgTemp(temperature) as avg_temp from sensor group by id");
        Table resultSqlTable_4 = tableEnv.sqlQuery("select id, avgTemp_2(temperature) as avg_temp from sensor group by id");

        // 输出结果
        tableEnv.toRetractStream(resultTable_4, Row.class).print("result_table_4");
        tableEnv.toRetractStream(resultSqlTable_3, Row.class).print("result_table_sql_3");
        tableEnv.toRetractStream(resultSqlTable_4, Row.class).print("result_table_sql_4");


        // TODO: 7.自定义表聚合函数
        MyAggTabTemp aggTabTemp = new MyAggTabTemp();
        // 注册udf函数
        tableEnv.createTemporarySystemFunction("udfAggTabTemp", aggTabTemp);

        // 7.1 table API
        Table resultTable_5 = sensorTable.groupBy($("id"))
                .flatAggregate(call("udfAggTabTemp", $("temperature")).as("temp", "temp_rank"))
                .select($("id"), $("temp"), $("temp_rank"));

        // 7.2 SQL
//        Table resultSqlTable_5 = tableEnv.sqlQuery("select id, temp, temp_rank from sensor, lateral table(udfAggTabTemp(temperature)) as aggTab(temp, temp_rank) group by id");

        // 输出结果
        tableEnv.toRetractStream(resultTable_5, Row.class).print("result_table_5");
//        tableEnv.toRetractStream(resultSqlTable_5, Row.class).print("reuslt_table_sql_5");

        // 开启执行
        senv.execute();
    }

    public static class HashCode extends ScalarFunction {

        private int factor = 13;

        public HashCode() {
        }

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String id) {
            return id.hashCode() * 13;
        }
    }

    // 实现自定义的表函数TableFunction
    @FunctionHint(output = @DataTypeHint("Row<word STRING, length INT>"))
    public static class Split extends TableFunction<Row> {
        // 定义属性, 分隔符
        private String separator = ",";

        public Split() {
        }

        public Split(String separetor) {
            this.separator = separetor;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(Row.of(s, s.length()));
            }
        }
    }

    public static class WeightAvgAccumulator {
        private double sum;
        private int count;

        public WeightAvgAccumulator(double sum, int count) {
            this.sum = sum;
            this.count = count;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    public static class AvgTemp extends AggregateFunction<Double, WeightAvgAccumulator> {

        @Override
        public Double getValue(WeightAvgAccumulator accumulator) {
            return accumulator.sum / accumulator.count;
        }

        @Override
        public WeightAvgAccumulator createAccumulator() {
            return new WeightAvgAccumulator(0.0, 0);
        }

        // 必须实现一个accumulate方法, 来数据之后更新状态
        // 这里方法名必须是这个, 且必须public
        // 累加器参数, 必须得是第一个参数, 随后的才是我们自己传入的参数
        public void accumulate(WeightAvgAccumulator accumulator, Double temp) {
            accumulator.sum += temp;
            accumulator.count += 1;
        }
    }

    public static class AvgTemp_2 extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }


    public static class AggTabAcc {
        public Double highestTemp;
        public Double secondTemp;
    }

    public static class MyAggTabTemp extends TableAggregateFunction<Tuple2<Double, Integer>, AggTabAcc> {

        @Override
        // 初始化状态
        public AggTabAcc createAccumulator() {
            AggTabAcc acc = new AggTabAcc();
            acc.highestTemp = Double.MIN_VALUE;
            acc.secondTemp = Double.MIN_VALUE;
            return acc;
        }

        // 对每过来的一个数据, 进行聚合操作
        public void accumulate(AggTabAcc acc, Double temp) {
            // 将当前温度值, 和状态中的最高温度和第二高温度进行比较, 如果大就替换
            if (temp > acc.highestTemp) {
                acc.highestTemp = temp;
            } else if (temp > acc.secondTemp) {
                acc.secondTemp = temp;
            }
        }

        // 实现一个数据输出的方法, 写入到结果表中
        public void emitValue(AggTabAcc acc, Collector<Tuple2<Double, Integer>> out) {
            out.collect(Tuple2.of(acc.highestTemp, 1));
            out.collect(Tuple2.of(acc.secondTemp, 2));
        }


    }
}
