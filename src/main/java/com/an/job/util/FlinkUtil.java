package com.an.job.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
public class FlinkUtil {
    private static StreamExecutionEnvironment env = null;

    private static ParameterTool parameterTool = null;
    private static void init(){
        if (Objects.isNull(env)){
            String flink_env = parameterTool.get("flink.env", "production");
            env = "develop".contains(flink_env)  ?
                        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())     // 开发环境 开启 webui
                    :   StreamExecutionEnvironment.getExecutionEnvironment();                               // 默认模式
        }
    }

    /**
     * 获取所有的 topics 的数据
     * @param args
     * @param deserialization
     * @return
     * @param <T>
     * @throws Exception
     */
    public static <T> DataStream<T> createKafkaStream(String[] args, Class<? extends DeserializationSchema<T>> deserialization) throws Exception {
        FlinkBuild(args);

        List<String> topics = Arrays.asList(parameterTool.get("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserialization.newInstance(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }

    /**
     * 可以获取Kafka 数据 的 Topic partition offset
     * @param args
     * @param deserialization
     * @return
     * @param <T>
     * @throws Exception
     */
    public static <T> DataStream<T> createKafkaStreamV2(String[] args, Class<? extends KafkaDeserializationSchema<T>> deserialization) throws Exception {
        FlinkBuild(args);
        List<String> topics = Arrays.asList(parameterTool.get("kafka.input.topics").split(","));

        Properties properties = parameterTool.getProperties();

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deserialization.newInstance(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }

    private static void FlinkBuild(String[] args) throws IOException {
        String setting = "conf/conf.properties";
        if (args.length > 0)
            setting = args[0];
        parameterTool = ParameterTool.fromPropertiesFile(setting);
        init();
        long chkInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String chkPath = parameterTool.get("checkpoint.path", null);
        // TODO 设置 checkPoint
        env.enableCheckpointing(chkInterval, CheckpointingMode.EXACTLY_ONCE);
        if (!Objects.isNull(chkPath)){
            if (chkPath.contains("hdfs://")){
                // TODO 状态后端 RocksDB
                // 状态后端
                env.setStateBackend(new RocksDBStateBackend(chkPath,true));
            }else if (chkPath.contains("file://")){
                // TODO 状态后端 本地缓存
                // 本地缓存
                env.setStateBackend(new FsStateBackend(chkPath));
            }else{
                System.out.println(log.warning("缓存路径 有误，请在 ====> checkpoint.path 重新填写"));
            }
        }
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

    public static ParameterTool getParameterTool() {
        return parameterTool;
    }
}