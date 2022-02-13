package com.hy.flink.streaming.window;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import java.time.Duration;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author hy
 * @date 2022/2/9 10:32 下午
 * @description
 */
public class WindowAssignerDemo {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        TimestampAssignerSupplier<Order> timestampAssignerSupplier = TimestampAssignerSupplier
                .of((SerializableTimestampAssigner<Order>) (element, recordTimestamp) -> element.getTimestamp());
        //水位线比实际时间少一个延迟时间窗口不关闭，水位线只增不减，等到具体时间到达推迟后的水位线则关闭
        //允许数据延迟2s到达
        WatermarkStrategy<Order> orderWatermarkStrategy = WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(timestampAssignerSupplier)
                //算完之后延迟时间重算
                .withIdleness(Duration.ofSeconds(2));
        source.assignTimestampsAndWatermarks(orderWatermarkStrategy).keyBy(Order::getOrderType)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .maxBy("price").print("TumblingEventTimeWindows");
        env.execute("TumblingEventTimeWindows");
    }
}
