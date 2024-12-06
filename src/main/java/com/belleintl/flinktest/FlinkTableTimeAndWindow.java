package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName: FlinkTableTimeAndWindow
 * @Description: FlinkTable时间语义
 * @Author: zhipengl01
 * @Date: 2021/11/7
 */
public class FlinkTableTimeAndWindow {
    public static void main(String[] args) throws Exception{
        // 1.创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings bsSet = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv, bsSet);

        //设置并行度
        senv.setParallelism(2);
        //设置水印周期生成时间
        senv.getConfig().setAutoWatermarkInterval(200L);
        //设置状态后端
//        senv.setStateBackend(new FsStateBackend("checkpointURI"));

        //设置checkpoint周期时间
        senv.enableCheckpointing(100L);
        //设置精确一次模式
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 2.获取DataStream数据源
        DataStream<String> inputDataStream = senv.readTextFile("D:\\Doc\\Test\\sensor.txt");

        //转换成POJO
        DataStream<SensorReading> sensorReadingDataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), Double.valueOf(fields[2]));
        });

        //生成水印
        DataStream<SensorReading> sensorWaterMarkDataStream = sensorReadingDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(2000L))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        //生成水印 使用lambda表达式
        DataStream<SensorReading> sensorWaterMarkDataStream2 = sensorReadingDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofMillis(200L))
                .withTimestampAssigner((SensorReading, recordTimestamp) -> SensorReading.getTimestamp()*1000L));

        // 3.将流转换成表, 定义时间特性
//        Table sensorTable = tableEnv.fromDataStream(sensorReadingDataStream, "id, timestamp as ts, temperature as temp, pt.proctime");

        String connectFileSystem = "create table dataTable2(" +
                "id varchar(20) not null, " +
                "temperature double, " +
//                "WATERMARK FOR pt as pt - INTERVAL '5' SECOND" +
                "ks as TO_TIMESTAMP( FROM_UNIXTIME(timestamp) ), " +
                "WATERMARK FOR ks as ks - INTERVAL '1' SECOND " +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'D:\\Doc\\Test\\sensor.txt'," +
                " 'format.type' = 'CSV')";

//        tableEnv.executeSql(connectFileSystem);

//        Table dataTable_2 = tableEnv.sqlQuery("select * from dataTable2");

        Table sensorTable_2 = tableEnv.fromDataStream(sensorWaterMarkDataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

        Table sensorTable_3 = tableEnv.fromDataStream(sensorWaterMarkDataStream, "id, timestamp.rowtime, temperature as temp");

        tableEnv.createTemporaryView("dataTable", sensorWaterMarkDataStream, $("id"), $("timestamp").rowtime().as("rt"), $("temperature"));
        Table dataTable = tableEnv.sqlQuery("select * from dataTable");

        // 窗口操作 详情可见官网 Application Development/Table API & SQL/Table API
        // 5.1 Group Window
        // table API
        Table dataTable2 = tableEnv.from("dataTable");
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy($("id"), $("tw"))
//                .select("id, id.count, temperature.avg, tw.end");
                .select($("id"), $("id").count(), $("temperature").avg(), $("tw").end());

        // 新版本采用下面这种形式
        Table resultTable2 = dataTable.window(Tumble.over(lit(10).second()).on($("rt")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count(), $("temperature").avg(), $("tw").end());


        // SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temperature) as avgTemp, tumble_end(rt, interval '10' second) " +
                "from dataTable group by id, tumble(rt, interval '10' second)");

        tableEnv.toRetractStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(resultTable2, Row.class).print("resultTable2");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable");

        // 5.2 Over Window
        //table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id, rt, id.count over ow, temperature.avg over ow");

        // 新版本建议采用下面这种形式
        Table overResult2 = dataTable.window(Over.partitionBy($("id")).orderBy($("rt")).preceding(rowInterval(2L)).as("ow"))
                .select($("id"), $("rt"), $("id").count().over($("ow")), $("temperature").avg().over($("ow")));

        // SQL
        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temperature) over ow " +
                "from dataTable " +
                "window ow as (partition by id order by rt rows between 2 preceding and current row)");

        tableEnv.toRetractStream(overResult, Row.class).print("overResult");
        tableEnv.toRetractStream(overResult2, Row.class).print("overResult2");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("overSqlResult");




//        sensorTable.printSchema();
//        tableEnv.toAppendStream(sensorTable, Row.class).print("sensorTable");
//        tableEnv.toRetractStream(sensorTable, Row.class).print("sensorTable");
//
//
        tableEnv.toAppendStream(sensorTable_2, Row.class).print("sensorTable2");
        tableEnv.toRetractStream(sensorTable_2, Row.class).print("sensorTable2");

        tableEnv.toAppendStream(sensorTable_3, Row.class).print("sensorTable3");
        tableEnv.toRetractStream(sensorTable_3, Row.class).print("sensorTable3");

        tableEnv.toAppendStream(dataTable, Row.class).print("dataTable");
//        tableEnv.toRetractStream(dataTable_2, Row.class).print("dataTable2");

        // 4.开启执行
        senv.execute();
    }
}
