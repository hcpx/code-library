package com.hy.flink.streaming.table;

import com.hy.flink.streaming.bean.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author hy
 * @Date 2022-02-13 19:19:43
 * @Description
 */
public class TableWaterMark {
    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                //数据Catalog
                .withBuiltInCatalogName("default_catalog")
                //数据库
                .withBuiltInDatabaseName("default_database").build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment, environmentSettings);
        Optional.ofNullable(TableDemo.class.getClassLoader().getResource("Demo.txt"))
                .map(URL::getFile)
                .ifPresent(path -> {
                    final DataStreamSource<String> dataStream = environment.readTextFile(path);
                    final SingleOutputStreamOperator<Order> stockStream = dataStream
                            .filter(value -> Objects.nonNull(value) && value.contains(","))
                            .map((MapFunction<String, Order>) value -> {
                                final String[] split = value.split(",");
                                return new Order(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
                            }).assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofMillis(2))
                                    .withTimestampAssigner((Order element, long recordTimestamp) -> element.getTimestamp()));

                    Table table = tableEnv.fromDataStream(stockStream, $("id"), $("price"), $("orderType"), $("eventTime").rowtime());
                    Table select = table.select($("id"), $("price"), $("eventTime"));
                    final DataStream<Tuple2<Boolean, Tuple3<String, Double, Timestamp>>> sqlTableDataStream = tableEnv.toRetractStream(select, TypeInformation.of(new TypeHint<Tuple3<String, Double, Timestamp>>() {
                    }));
                    sqlTableDataStream.print("sqlTable");
                });
        environment.execute();
    }
}
