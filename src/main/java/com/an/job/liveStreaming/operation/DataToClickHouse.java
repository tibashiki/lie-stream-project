package com.an.job.liveStreaming.operation;

import com.an.job.kafka.MyKafkaDeserializationSchema;
import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.udf.JsonToDataBeanV2;
import com.an.job.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 将数据写入clickhouse
 */
public class DataToClickHouse {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> dataStream = FlinkUtil.createKafkaStreamV2(args, MyKafkaDeserializationSchema.class);

        dataStream.process(new JsonToDataBeanV2())
                .map(new MapFunction<DataBean, DataBean>() {
                    @Override
                    public DataBean map(DataBean dataBean) throws Exception {
                        Long timestamp = dataBean.getTimestamp();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");
                        String format = dateFormat.format(new Date(timestamp));
                        String[] split = format.split("-");
                        dataBean.setDate(split[0]);
                        dataBean.setHour(split[1]);
                        return dataBean;
                    }
                })
                .addSink(JdbcSink.sink(
                "insert into tb_user_event values (?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setString(2, t.getDeviceId());
                    ps.setString(3, t.getEventId());
                    ps.setInt(4, t.getIsNew());
                    ps.setString(5,t.getOsName());
                    ps.setString(6, t.getProvince());
                    ps.setString(7, t.getReleaseChannel());
                    ps.setString(8, t.getDeviceType());
                    ps.setLong(9, t.getTimestamp());
                    ps.setString(10,t.getDate());
                    ps.setString(11,t.getHour());
                    ps.setLong(12,System.currentTimeMillis()/1000);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(FlinkUtil.getParameterTool().getInt("clickhouse.batch.size"))
                        .withBatchIntervalMs(FlinkUtil.getParameterTool().getInt("clickhouse.batch.interval"))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node01:8123/liveStreaming")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build()));


        FlinkUtil.getEnv().execute();
    }
}
