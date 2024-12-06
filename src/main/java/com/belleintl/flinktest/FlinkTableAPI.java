package com.belleintl.flinktest;

import com.belleintl.finkbean.SensorReading;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: FlinkTableAPI
 * @Description: Flink的Table API
 * @Author: zhipengl01
 * @Date: 2021/10/31
 */
public class FlinkTableAPI {
    public static void main(String[] args) throws Exception{
        // 1.创建环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        senv.setParallelism(2);

        // 2.获取数据
        DataStream<String> inputStream = senv.readTextFile("D:\\Doc\\Test\\sensor.txt");

        // 3.转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), Double.valueOf(field[2]));
        });

        // 4.创建表环境
        EnvironmentSettings bsSet = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv, bsSet);

        // 5.基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream); // 将DataStream转换成表

        // 表的创建: 连接外部系统, 获取数据
        String filePath = "D:\\Doc\\Test\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))   // 定义到文件系统的连接
                .withFormat(new Csv())  // 定义以CSV格式进行数据格式化
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )   // 定义表结构
                .createTemporaryTable("inputTable");  // 创建临时表

        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 查询转换
        // 简单转换
        Table simpleResult = inputTable.filter($("id").isEqual("sensor_2"))
                .select($("id"), $("temperature"));

        // 聚合查询
        Table aggregationResult = inputTable.groupBy($("id"))
                .select($("id"), $("temperature").avg().as("avg_temp"), $("id").count().as("cnt"));

        // SQL查询操作
        String simpleSql = "select id, temperature from inputTable where id = 'sensor_2'";
        String aggSql = "select id, avg(temperature) as avg_temp, count(id) as cnt from inputTable group by id";

        Table simpleResult_2 = tableEnv.sqlQuery(simpleSql);
        Table aggregationResult_2 = tableEnv.sqlQuery(aggSql);

        // 打印输出
        tableEnv.toAppendStream(simpleResult, Row.class).print();
        tableEnv.toRetractStream(aggregationResult, Row.class).print();
        tableEnv.toAppendStream(simpleResult_2, Row.class).print();
        tableEnv.toRetractStream(aggregationResult_2, Row.class).print();


        // 6.调用table API进行转换操作
        Table resultTable = dataTable.select("id, temperature")
                .where("id = 'sensor_1'");

        Table resultTable_2 = dataTable.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_id"));

        Table resultTable_5 = dataTable.groupBy($("id"))
                .select($("id"), $("temperature").avg().as("avg_temp"));


        // 7.执行SQL
        tableEnv.createTemporaryView("sensor", dataStream);

        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        Table dataTable_2 = tableEnv.from("sensor");

        Table resultTable_4 = dataTable_2.filter($("id").isNotEqual("sensor_1"))
                .groupBy($("id"))
                .select($("id"), $("temperature").avg().as("avg_temp"));

        // 8.打印输出
        /**
         * 里面的false表示上一条保存的记录被删除，true则是新加入的数据
         * 所以Flink的Table API在更新数据时，实际是先删除原本的数据，再添加新数据。
         */
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultTable_2, Row.class).print("result2");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        tableEnv.toRetractStream(resultTable_5, Row.class).print("result5");
        tableEnv.toRetractStream(resultTable_4, Row.class).print("result4");

        // 开启执行
        senv.execute();
    }
}
