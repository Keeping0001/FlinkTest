package com.belleintl.ecommerce.user_analysis.flinkstream;

import com.belleintl.ecommerce.user_analysis.bean.ItemViewCount;
import com.belleintl.ecommerce.user_analysis.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import org.apache.flink.util.Collector;
import scala.Int;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: HotItems
 * @Description: 热点商品统计
 * @Author: zhipengl01
 * @Date: 2022/1/4
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1.创建FlinkStream环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        senv.setParallelism(1);
        // 设置周期性水印时间
        senv.getConfig().setAutoWatermarkInterval(200L);

        // 设置状态后端
//        senv.setStateBackend(new FsStateBackend(""));
        senv.setStateBackend(new MemoryStateBackend()); //一般用于测试代码中

        // 设置checkpoint时间
        senv.enableCheckpointing(200L);

        // 设置精确一致模式
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint处理超时时间
        senv.getCheckpointConfig().setCheckpointTimeout(1000L);

        // 2.获取数据
        DataStream<String> inputStream = senv.readTextFile("D:\\Doc\\Test\\UserBehavior.txt");
        // 转换成POJO, 并分配时间戳和watermark
        DataStream<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] feilds = line.split(",");
            return new UserBehavior(new Long(feilds[0]), Long.valueOf(feilds[1]), Integer.valueOf(feilds[2]), feilds[3], Long.valueOf(feilds[4]));
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                }
        ));

        // 3.分组开窗, 得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = userBehaviorDataStream
                // 过滤只保留pv行为数据
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                // 按照商品id进行分组
                .keyBy(UserBehavior::getItemId)
                // 滑动窗口
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());  // 增量聚合

        // 4.收集同一窗口的所有商品的count数据, 排序输出top n
        DataStream<String> resultStream = windowAggStream
                // 按照窗口分组
                .keyBy(ItemViewCount::getWindowEnd)
                // 用自定义处理函数排序取前5
                .process(new TopNHotItem(5));

        resultStream.print();

        // 5.开始执行
        senv.execute();
    }

    /**
     * 自定义增量聚合函数 AggregateFunction
     * IN: The type of the values that are aggregated (input values)
     * ACC: The type of the accumulator累加器 (intermediate aggregate state)
     * OUT: The type of the aggregated result
     */

    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            /* 累加器初始值为0 */
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
     * 自定义全窗口函数 WindowFunction
     * IN: 输入的类型
     * OUT: 输出的类型
     * Key: key的类型
     * W <: Window 窗口类型
     */
    /* WindowFunction 输入数据来自于 AggregateFunction, 在窗口结束的时候先执行Aggregate对象的getResult, 然后再执行 自己的apply对象 */
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow>{

        @Override
        public void apply(Long itemID, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemID, windowEnd, count));
        }
    }

    /**
     * 实现自定义KeyedProcessFunction
     * Key: key的类型
     * IN: 输入的类型
     * OUT: 输出的类型
     */
    public static class TopNHotItem extends KeyedProcessFunction<Long, ItemViewCount, String> {

        // 定义属性, TopN的大小
        private Integer topN;

        // 定义状态列表, 保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class)
            );
        }

        public TopNHotItem(Integer topN) {
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
            resultBuilder.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append(System.lineSeparator());

            // 遍历列表, 取top n输出
            for(int i=0; i<Math.min(topN, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("No ").append(i+1).append(":")
                        .append(" 商品ID= ").append(currentItemViewCount.getItemId())
                        .append(" 热门度= ").append(currentItemViewCount.getCount())
                        .append(System.lineSeparator()); // 按照操作系统返回对应的行分隔符
            }

            resultBuilder.append("===============================").append(System.lineSeparator());

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
