package com.hy.flink.streaming.sink;

import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author hy
 * @date 2022/2/8 2:36 下午
 * @description
 */
@Slf4j
public class SocketStream {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);
        inputDataStream.print();
        log.info("SocketStream start");
        final DataStream<Tuple2<String, Integer>> wordCounts = inputDataStream
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    final String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        wordCounts.print();
        wordCounts.writeToSocket(host, port, (SerializationSchema<Tuple2<String, Integer>>) element -> (element.f0 + "-" + element.f1)
                .getBytes(StandardCharsets.UTF_8));
        environment.execute("stream word count");
    }


}
