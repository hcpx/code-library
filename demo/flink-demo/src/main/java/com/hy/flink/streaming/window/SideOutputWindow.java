package com.hy.flink.streaming.window;

import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author hy
 * @date 2022/2/12 3:46 下午
 * @description
 */
public class SideOutputWindow {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");
        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);
        final OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };
        SingleOutputStreamOperator<Integer> mainDataStream = inputDataStream.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out)
                    throws Exception {
                // 发送数据到主要的输出
                out.collect(Integer.valueOf(value));
                // 发送数据到旁路输出
                ctx.output(outputTag, "sideout-" + value);
            }
        });
        mainDataStream.print("main");
        mainDataStream.getSideOutput(outputTag).print("side");
        environment.execute();
    }
}
