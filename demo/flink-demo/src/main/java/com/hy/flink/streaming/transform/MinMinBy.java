package com.hy.flink.streaming.transform;

import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hy
 * @date 2022/2/8 8:08 下午
 * @description
 */
public class MinMinBy {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据源
        List<Tuple3<Integer,Integer,Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,5,6));
        data.add(new Tuple3<>(0,3,5));
        data.add(new Tuple3<>(1,1,9));
        data.add(new Tuple3<>(1,2,8));
        data.add(new Tuple3<>(1,3,10));
        data.add(new Tuple3<>(1,2,9));

        DataStreamSource<Tuple3<Integer,Integer,Integer>> items = env.fromCollection(data);
        items.keyBy(value->value.f0).min(2).print("min");
        items.keyBy(value->value.f0).minBy(2).print("min by");

        env.execute("defined streaming source");
    }
}
