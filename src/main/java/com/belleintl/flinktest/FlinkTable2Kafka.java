package com.belleintl.flinktest;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName: FlinkTable2Kafka
 * @Description: Flink TableAPI连接kafka
 * @Author: zhipengl01
 * @Date: 2021/10/31
 */
public class FlinkTable2Kafka {
    public static void main(String[] args) throws Exception {
        // 1.准备执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings bsSet = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv, bsSet);

        //设置并行度
        senv.setParallelism(2);
        //设置水印周期生成时间
        senv.getConfig().setAutoWatermarkInterval(400L);
        //设置状态后端
        senv.setStateBackend(new FsStateBackend("checkpointURI"));

        //设置checkpoint周期时间
        senv.enableCheckpointing(200L);
        //设置精确一次模式
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        //设置重启策略
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000L));

        // 2.连接kafka, 读取数据
        tableEnv.connect(new Kafka()
                            .version("universal")
                            .topic("sensor")
                            .property("zookeeper.connect", "localhost:2181")
                            .property("bootstrap.servers", "localhost:9092")
                        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");


        String connectKafka = "Create table InputTable (" +
                "id String, " +
                "timestamp BigInt, " +
                "temperature Double" +
                ") with ( " +
                " 'connect' = 'kafka', " +
                " 'topic' = 'sensor', " +
                " 'properties.bootstrap.servers' = 'localhost:9092', " +
                " 'format' = 'csv'" +
                ")";

        TableResult tableResult = tableEnv.executeSql(connectKafka);

        Table table = tableEnv.sqlQuery("select * from inputTable");
        Table table_2 = tableEnv.sqlQuery("select * from InputTable");

        Table sensorTable = tableEnv.from("inputTable");

        // 3.查询转换
        Table aggTable = table.groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"), $("temperature").avg().as("avg_temp"));

        Table simpleTable = table_2.filter($("id").isEqual("sensor_1"))
                .select($("id"), $("temperature"));

        Table simpleSensorTable = sensorTable.filter($("id").isNotEqual("sensor_2"))
                .select($("id"), $("temperature"));

        // 4.建立kafka连接, 输出到不同的topic中
        String sinkKafka = "create table outputTable(" +
                "id String, " +
                "temperature Double" +
                ") with (" +
                " 'connect' = 'kafka'," +
                " 'topic' = 'sink_sensor'," +
                " 'properties.bootstrap.servers' = 'localhost:9092'," +
                " 'format' = 'csv'";

        tableEnv.executeSql("insert into outputTable select * from " + simpleTable);

        // 5.开始执行
        senv.execute();
    }
}
