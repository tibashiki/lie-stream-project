package com.an.job.liveStreaming.operation.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestClickHouseJDBCSink {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(new Configuration());

        DataStreamSource<String> line = env.socketTextStream("127.0.0.1", 13345);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = line.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        tpStream.addSink(JdbcSink.sink(
                "insert into test_user values (?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setString(2, t.f1);
                    ps.setString(3, t.f2);
                },
                JdbcExecutionOptions.builder().withBatchSize(2).withBatchIntervalMs(5000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node01:8123/test")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build()));
        env.execute();
    }
}
