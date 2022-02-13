package com.hy.flink.streaming.table;

/**
 * @author hy
 * @date 2022/2/12 5:54 下午
 * @description
 */
public class TableDemo {

    public static void main(String[] args) {
        // create a TableEnvironment for specific planner batch or streaming TableEnvironment tableEnv = ...; // create an input Table tableEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )"); // register an output Table tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )"); // create a Table object from a Table API query Table table2 = tableEnv.from("table1").select(...); // create a Table object from a SQL query Table table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ... "); // emit a Table API result Table to a TableSink, same for SQL result TableResult tableResult = table2.executeInsert("outputTable"); tableResult...
    }
}
