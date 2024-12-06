package com.belleintl.clickhouse;

import com.alibaba.fastjson.JSON;
import com.belleintl.finkbean.Mail;
import com.belleintl.flinktest.FlinkTableFunctions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName: FlinkSinkClickhouse
 * @Description: Flink写入到Clickhouse中
 * @Author: zhipengl01
 * @Date: 2022/6/1
 */
public class FlinkSinkClickhouse {
    public static void main(String[] args) throws Exception {
        // 1.准备执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        senv.setParallelism(2);
        // 设置checkpoint时间
        senv.enableCheckpointing(5000L);
        // 设置周期性水印时间
        senv.getConfig().setAutoWatermarkInterval(2000L);
        // 设置状态后端
        senv.setStateBackend(new FsStateBackend(""));
//        senv.setStateBackend(new MemoryStateBackend());  // 内存级状态后端, 一般用在测试中

        // 设置精确一致模式
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint超时时间
        senv.getCheckpointConfig().setCheckpointTimeout(3000L);
        // 最大允许处理几个checkpoint
        senv.getCheckpointConfig().setMaxConcurrentCheckpoints(4);

        // source
        String topic = "test_process";

        // kafka环境配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.30.43:9092");
        properties.setProperty("zookeeper.connect", "10.0.30.43:2181");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //key 反序列化
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化

        // 2.定义Flink kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromGroupOffsets();
        consumer.setStartFromEarliest();  // 设置每次都从头开始消费

        // 3.添加source数据流
        DataStreamSource<String> source = senv.addSource(consumer);
        System.out.println(source);
        SingleOutputStreamOperator<Mail> dataStream = source.map(new MapFunction<String, Mail>() {
            @Override
            public Mail map(String value) throws Exception {
                HashMap<String, String> hashMap = JSON.parseObject(value, HashMap.class);
                System.out.println(hashMap);
                String appKey = hashMap.get("appKey");
                String appVersion = hashMap.get("appVersion");
                String deviceId = hashMap.get("deviceId");
                String phoneNo = hashMap.get("phoneNo");

                return new Mail(appKey, appVersion, deviceId, phoneNo);
            }
        });

        dataStream.print();

        // 4.sink
        String sql = "insert into table test.ods_countlyV2(app_key, app_version, device_id, phone_no) values (?, ?, ?, ?)";
        MyClickhouseUtil ckSink = new MyClickhouseUtil(sql);
        dataStream.addSink(ckSink);

        // 5.开启执行
        senv.execute();
    }
}
