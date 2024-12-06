package com.belleintl.flinktest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: FlinkSourece_Kafka
 * @Description: Flink数据源_Kafka
 * @Author: zhipengl01
 * @Date: 2021/9/3
 */
public class FlinkSourece_Kafka {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度, 默认值=计算机cpu核数
        senv.setMaxParallelism(32);
        senv.setParallelism(4);

        //3.kafka环境配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.30.43:9092");
        properties.setProperty("group.id", "0");
        properties.setProperty("zookeeper.connect", "10.0.30.43:2181");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //key 反序列化
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化
        properties.setProperty("auto.offset.reset", "latest");

        //4.flink添加外部数据源
        DataStream<String> dataStream = senv.addSource(new FlinkKafkaConsumer<String>("db_wms0_stk_book_asyn_msg", new SimpleStringSchema(), properties));

        //打印输出
        dataStream.print();

        //执行任务
        senv.execute();
    }
}
