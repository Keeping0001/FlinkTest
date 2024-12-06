package com.belleintl.ecommerce.user_analysis.flinkstream;

import com.belleintl.ecommerce.user_analysis.bean.ItemViewCount;
import com.belleintl.ecommerce.user_analysis.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: HotItemsFromKafka
 * @Description: 热点商品统计-数据来源kafka
 * @Author: zhipengl01
 * @Date: 2022/1/20
 */
public class HotItemsFromKafka {
    public static void main(String[] args) throws Exception{
        // 1.创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        senv.setParallelism(2);

        // 设置watermark
        senv.getConfig().setAutoWatermarkInterval(200L);

        // 设置状态后端
//        senv.setStateBackend(new FsStateBackend(""));
        senv.setStateBackend(new MemoryStateBackend());

        // 设置checkpoint时间
        senv.enableCheckpointing(200L);

        // 设置精确一致性
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint超时时间
        senv.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 2.获取数据-kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        // 下面是一些次要参数
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从kafka中消费数据
        DataStream<String> inputDataStream = senv.addSource(new FlinkKafkaConsumer<>("hotitems", new SimpleStringSchema(), properties));
        // 转换成POJO, 并分配时间戳和watermark
        DataStream<UserBehavior> userBehaviorDataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                }
        ));

        // 3.处理数据
        DataStream<ItemViewCount> windowAggStream = userBehaviorDataStream
                // 过滤只保留pv行为数据
                .filter(uerbehavior -> "pv".equals(uerbehavior.getBehavior()))
                // 按照商品id进行分组
                .keyBy(UserBehavior::getItemId)
                // 滑动窗口
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());    // 增量聚合

        // 4.收集同一窗口所有商品的count数据, 排序输出topN
        DataStream<String> resultStream = windowAggStream
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopHotItem(5));

        resultStream.print();

        senv.execute();
    }

    /**
     * 实现自定义递增函数
     * IN: 输入的类型
     * ACC: 累加器的类型
     * OUT: 输出的类型
     */
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            /* 累加器初始化值为0 */
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            /* 来一条数据执行一次 */
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            /* 在窗口结束时执行一次, 返回的是一个累加的数值 */
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            /* 如果有分区, 合并两个分区的数据 */
            return a + b;
        }
    }

    /**
     * 实现自定义窗口函数
     * WindowFunction输入数据来自于AggregateFunction, 在窗口结束的时候, 先执行Aggregate对象的getResult, 在执行自己的apply
     * IN: 输入类型
     * OUt: 输出类型
     * KEY: key类型
     * W: window类型
     */
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow>{

        @Override
        public void apply(Long itemID, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long windowEnd = window.getEnd();
            long count = input.iterator().next();
            out.collect(new ItemViewCount(itemID, windowEnd, count));
        }
    }

    /**
     * 实现自定义KeyedProcessFunction
     * Key: key类型
     * In: 输入类型
     * Out: 输出类型
     */
    public static class TopHotItem extends KeyedProcessFunction<Long, ItemViewCount, String> {

        // 定义属性, TopN的大小
        private Integer topN;

        // 定义状态列表, 保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-view-count-state", ItemViewCount.class)
            );
        }

        public TopHotItem(Integer topN) {
            this.topN = topN;
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据, 存入list, 并注册定时器
            itemViewCountListState.add(value);
            // 模拟等待, 所以这里时间设置比较短(1ms)
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发, 当前已收集到所有数据, 排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            // 从多到少
            itemViewCounts.sort((a, b) -> -Long.compare(a.getCount(), b.getCount()));
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=====================").append(System.lineSeparator());
            resultBuilder.append("窗口结束时间: ").append(new Timestamp(timestamp -1)).append(System.lineSeparator());

            // 遍历列表 取出top N输出
            for(int i=0; i<Math.min(topN, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("No ").append(i+1).append(":")
                        .append(" 商品ID= ").append(currentItemViewCount.getItemId())
                        .append(" 热门度= ").append(currentItemViewCount.getCount())
                        .append(System.lineSeparator());
            }

            resultBuilder.append("===============================").append(System.lineSeparator());

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
