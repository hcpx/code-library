package com.hy.flink.streaming.window;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;


/**
 * @author hy
 * @date 2022/2/9 10:32 下午
 * @description
 */
public class WindowFunctionDemo {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        TimestampAssignerSupplier<Order> timestampAssignerSupplier = TimestampAssignerSupplier
                .of((SerializableTimestampAssigner<Order>) (element, recordTimestamp) -> element.getTimestamp());
        //这里打印有点不正确,主要查看每个window的start和end time
        source.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forMonotonousTimestamps()
                .withTimestampAssigner(timestampAssignerSupplier))
                .keyBy(Order::getOrderType)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .apply(new WindowFunction<Order, Tuple4<String, Long, Long, Long>, String, TimeWindow>() {
//                    @Override
//                    public void apply(String key, TimeWindow window, Iterable<Order> input, Collector<Tuple4<String, Long, Long, Long>> out)
//                            throws Exception {
//                        long count = IterableUtils.toStream(input).count();
//                        out.collect(new Tuple4<>(key, count, window.getStart(), window.getEnd()));
//                    }
//                })
                //这里及其坑爹无法使用lambda（1.12.7），应该是个bug并且已经修复了在（1.14.2）上可以运行
//                .apply((String key,TimeWindow window,Iterable<Order> input,Collector<Tuple4<String, Long, Long, Long>> out) -> {
//                    long count = IterableUtils.toStream(input).count();
//                    out.collect(new Tuple4<>(key, count, window.getStart(), window.getEnd()));
//                })
//                .returns(Types.TUPLE(Types.STRING,Types.LONG,Types.LONG,Types.LONG))
                //效果和aggregate的一样但是这个很耗费内存
//                .apply(new MyAvgFunction())
//                .aggregate(new MyAggregateAvgFunction())
                .apply(new MyWindowFunction())
                .print("TumblingEventTimeWindows");
        env.execute("TumblingEventTimeWindows");
    }

    static class MyWindowFunction implements WindowFunction<Order, Tuple4<String, Long, Long, Long>, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Order> input, Collector<Tuple4<String, Long, Long, Long>> out)
                throws Exception {
            long count = IterableUtils.toStream(input).count();
            out.collect(new Tuple4<>(key, count, window.getStart(), window.getEnd()));
        }
    }

    static class MyAvgFunction implements WindowFunction<Order, Tuple2<String, Double>, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Order> input, Collector<Tuple2<String, Double>> out) throws Exception {
            long count = IterableUtils.toStream(input).count();
            double sum = IterableUtils.toStream(input).mapToDouble(Order::getPrice).sum();
            out.collect(new Tuple2<>(key, sum / count));
        }
    }

    //微批处理
    static class MyAggregateAvgFunction implements AggregateFunction<Order, Tuple3<String, Integer, Double>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Integer, Double> createAccumulator() {
            return new Tuple3<>("", 0, 0D);
        }

        @Override
        public Tuple3<String, Integer, Double> add(Order value, Tuple3<String, Integer, Double> accumulator) {
            return new Tuple3<>(value.getOrderType(), accumulator.f1 + 1, accumulator.f2 + value.getPrice());
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Integer, Double> accumulator) {
            return new Tuple2<>(accumulator.f0, accumulator.f2 / accumulator.f1);
        }

        @Override
        public Tuple3<String, Integer, Double> merge(Tuple3<String, Integer, Double> a, Tuple3<String, Integer, Double> b) {
            return new Tuple3<>(a.f0,a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
