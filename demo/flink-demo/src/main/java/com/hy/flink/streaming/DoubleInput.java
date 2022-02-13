package com.hy.flink.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hy
 * @date 2022/1/29 4:22 下午
 * @description
 */
public class DoubleInput {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);
        dataStream.map(value -> 2 * value).setParallelism(1).print("doubleInput");
        env.execute("doubleInput");
    }
}
