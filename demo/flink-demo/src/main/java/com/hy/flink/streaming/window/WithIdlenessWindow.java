package com.hy.flink.streaming.window;

import com.hy.flink.streaming.window.WaterMarkWindowDemo.Demo;
import java.time.Duration;
import java.util.Objects;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author hy
 * @date 2022/2/12 4:23 下午
 * @description
 */
public class WithIdlenessWindow {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        WatermarkStrategy<Demo> demoWatermarkStrategy = WatermarkStrategy.<Demo>forMonotonousTimestamps()
                // 主要用于数据倾斜问题，如果parallelism较多那么所有slots都必须要达到指定水位线才会触发计算，等待这个时间后可以避免这种情况发生
                .withIdleness(Duration.ofSeconds(10))
                .withTimestampAssigner((Demo element, long recordTimestamp) -> element.getTime());

        environment.socketTextStream(host, port)
                .filter(value -> Objects.nonNull(value) && value.contains(","))
                .map(value -> {
                    String[] split = value.split(",");
                    return new Demo(split[0], Long.valueOf(split[1]));
                })
                .assignTimestampsAndWatermarks(demoWatermarkStrategy)
                .keyBy(Demo::getName)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .max("time")
                .print();
        environment.execute();
    }

}
