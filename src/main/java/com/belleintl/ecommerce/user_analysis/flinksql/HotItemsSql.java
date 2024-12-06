package com.belleintl.ecommerce.user_analysis.flinksql;

import com.belleintl.ecommerce.user_analysis.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName: HotItemsSql
 * @Description: FlinkSQL版本热门商品
 * @Author: zhipengl01
 * @Date: 2022/2/10
 */
public class HotItemsSql {
    public static void main(String[] args) throws Exception{
        // 1.设置环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        senv.setParallelism(2);

        // 设置watermark
        senv.getConfig().setAutoWatermarkInterval(200L);

        // 设置checkpoint
        senv.enableCheckpointing(2000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置状态后端
//        senv.setStateBackend(new FsStateBackend(""));
        senv.setStateBackend(new MemoryStateBackend()); // 测试中常使用

        // 设置checkpoint超时时间
        senv.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 2.获取数据
        DataStream<String> inputStream = senv.readTextFile("D:\\Doc\\Test\\UserBehavior.txt");
        // 转换成POJO, 并分配时间戳和watermark
        DataStream<UserBehavior> userBehaviorDateStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            long timeStamp = Long.parseLong(fields[4]) + 8*60*60;
            return new UserBehavior(new Long(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), fields[3], timeStamp);
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200L, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(UserBehavior element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                )
        );

        // 3.创建表执行环境, 使用BLINK版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv, settings);

        // 将流转换成表
        Table dataTable = tableEnv.fromDataStream(userBehaviorDateStream, $("userId"), $("itemId"), $("categoryId"), $("behavior"), $("timestamp").rowtime().as("ts"));

        // 分组开窗
        Table windowAggTable = dataTable.filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hour()).every(lit(5).minutes()).on($("ts")).as("w"))
                .groupBy($("itemId"), $("w"))
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));

        // 利用开窗函数, 对count值进行排序, 并获取Row Number, 并得到Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg",aggStream,$("itemId"),$("windowEnd"),$("cnt"));

        Table resultTable = tableEnv.sqlQuery("select * from " +
                "(select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "from agg) " +
                "where row_num <= 5");

        // 纯SQL
        tableEnv.createTemporaryView("dataTable", userBehaviorDateStream, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));

        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "(select *, row_number() over (partition by windowEnd order by cnt desc) as row_num " +
                "from ( " +
                "   select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd" +
                "   from dataTable" +
                "   where behavior = 'pv'" +
                "   group by itemId, HOP(ts, interval '5' minute, interval '1' hour) " +
                "   )" +
                ")" +
                "where row_num <= 5");

        tableEnv.toAppendStream(dataTable, Row.class).print("dataTable->");

        tableEnv.toRetractStream(windowAggTable, Row.class).print("windowAggTable->");
        tableEnv.toRetractStream(resultTable, Row.class).print("resultTable->");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable->");

        senv.execute();
    }
}
