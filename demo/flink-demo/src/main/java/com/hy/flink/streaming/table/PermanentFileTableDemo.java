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
import org.apache.flink.table.functions.ScalarFunction;

import java.net.URL;
import java.util.Objects;
import java.util.Optional;

/**
 * @Author hy
 * @Date 2022-02-13 17:17:56
 * @Description
 */
public class PermanentFileTableDemo {
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

                    //创建临时表 计算任务完成就回收
//                    tableEnv.createTemporaryView("orderTable",stockStream);
                    //创建永久表 除非显示删除否则一直存在
                    String sql =
                            "create table orderTable (" +
                                    " id varchar, " +
                                    " orderType varchar, " +
                                    " price double, " +
                                    " `timestamp` bigint " +
                                    ") with (" +
                                    " 'connector.type' = 'filesystem', " +
                                    " 'format.type' = 'csv', " +
                                    " 'connector.path' = 'D://flinkTable')";
                    try {
                        tableEnv.executeSql(sql);
//                        String explain = tableEnv.explainSql(sql);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    final Table stockTable = tableEnv.fromDataStream(stockStream);
                    stockTable.executeInsert("orderTable");
                    //临时表和永久表同时存在那么只访问临时表，临时表不存在`default_catalog`.`default_database`的概念会报错
                    final Table sqlTable = tableEnv.sqlQuery("select id,orderType,avg(price) as priceavg from orderTable where orderType='UDFOrder' group by id,orderType");
                    final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlTableDataStream = tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
                    }));
                    sqlTableDataStream.print("sqlTable");
                });
        environment.execute();
    }

    //自定义函数
    public static class HashCode extends ScalarFunction {
        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        //自定义函数有好些都有这个问题，没有接口直接重写这个方法即可，没有原因
        //多个参数转成一个结果，参数和返回值都不能为Object只能是固定的参数如：String Integer等等
        public int eval(String s) {
            return s.hashCode() * factor;
        }
    }


}
