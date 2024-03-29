package com.hy.flink.streaming.table;

import com.hy.flink.streaming.bean.Order;
import lombok.SneakyThrows;
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
import java.util.Objects;
import java.util.Optional;

/**
 * @author hy
 * @date 2022/2/12 5:54 下午
 * @description
 */
public class TableDemo {

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
                            });
                    final Table stockTable = tableEnv.fromDataStream(stockStream);

                    //是否覆盖  toRetractStream 是，数据结果中前边boolean true未覆盖 false覆盖
                    //         toAppendStream  否，仅叠加
                    //方式1
//                    final Table table = stockTable.groupBy($("id"), $("orderType"))
//                            .select($("id"), $("orderType"), $("price").avg().as("priceavg"))
//                            .where($("orderType").isEqual("UDFOrder"));
//                    final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlDataStream = tableEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
//                    }));
//                    sqlDataStream.print("sql");
                    //转换成流
                    //方式2
                    tableEnv.createTemporaryView("orderTable", stockTable);
                    String sql = "select id,orderType,avg(price) as priceavg from orderTable where orderType='UDFOrder' group by id,orderType";
                    final Table sqlTable = tableEnv.sqlQuery(sql);
                    final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlTableDataStream = tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
                    }));
                    sqlTableDataStream.print("sqlTable");
                });
        environment.execute();

    }
}
