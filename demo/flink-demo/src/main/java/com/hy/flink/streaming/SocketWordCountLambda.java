package com.hy.flink.streaming;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author hy
 * @date 2022/1/28 7:13 下午
 * @description
 */
public class SocketWordCountLambda {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        final int port = parameterTool.getInt("port");

        final DataStreamSource<String> inputDataStream = environment.socketTextStream(host, port);
        inputDataStream.print();
        //擦，这儿有个泛型擦除
        DataStream<Tuple2<String, Integer>> singleOutputStreamOperator = inputDataStream
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    final String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(value -> value.f0)
                .sum(1);
        singleOutputStreamOperator.print();
        environment.execute("stream word count");
    }
}
