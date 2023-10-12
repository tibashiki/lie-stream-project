package com.an.job.udf;

import com.alibaba.fastjson.JSON;
import com.an.job.liveStreaming.pojo.test;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JsonToDataBean extends ProcessFunction<String, test> {
    @Override
    public void processElement(String string, ProcessFunction<String, test>.Context context, Collector<test> collector) throws Exception {
        try {
            test dataBean = JSON.parseObject(string, test.class);
            collector.collect(dataBean);
        }catch (Exception ignored){
            // TODO 解析失败
        }
    }
}
