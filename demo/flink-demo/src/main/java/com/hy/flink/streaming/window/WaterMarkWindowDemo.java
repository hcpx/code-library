package com.hy.flink.streaming.window;

import java.io.Serializable;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author hy
 * @date 2022/2/10 8:33 下午
 * @description
 */
public class WaterMarkWindowDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //watermark 自动提交时间
//      environment.getConfig().setAutoWatermarkInterval(10 * 1000);
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        //侧输出流，如果延迟时间以及延迟计算都不能满足要求那么就会到侧输出流
        final OutputTag<Demo> outputStreamOutPutTag = new OutputTag<Demo>("lateDate"){};

        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);
        //水位线本来是10 现在要推高到12才会执行，之后在22之前请求10内的数据都会重新计算
        WatermarkStrategy<Demo> demoWatermarkStrategy = WatermarkStrategy.<Demo>forBoundedOutOfOrderness(Duration.ofMillis(2))
//        WatermarkStrategy<Demo> demoWatermarkStrategy = WatermarkStrategy.forGenerator((ctx) -> new MyWaterMark())
                // 主要用于数据倾斜问题，如果parallelism较多那么所有slots都必须要达到指定水位线才会触发计算，等待这个时间后可以避免这种情况发生
//                .withIdleness(Duration.ofMillis(2))
                .withTimestampAssigner((Demo element, long recordTimestamp) -> element.getTime());
        SingleOutputStreamOperator<Demo> time = inputDataStream.map(value -> {
            if (!value.contains(",")) {
                return null;
            }
            String[] split = value.split(",");
            return new Demo(split[0], Long.valueOf(split[1]));
        })
                .assignTimestampsAndWatermarks(demoWatermarkStrategy)
                .keyBy(Demo::getName)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
//                //1s之内都会触发重新计算，前边是毫秒
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(outputStreamOutPutTag)
                .maxBy("time");

        time.getSideOutput(outputStreamOutPutTag).print("lateDate");
        time.print("max by");
        environment.execute("stream word count");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Demo implements Serializable {

        private String name;
        private Long time;
    }

    @Slf4j
    static class MyWaterMark implements WatermarkGenerator<Demo> {

        @Override
        public void onEvent(Demo event, long eventTimestamp, WatermarkOutput output) {
            Long time = event.getTime();
            log.info("提交 watermark:{}", time);
            output.emitWatermark(new Watermark(time));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            log.info("自动提交watermark");
        }
    }
}
