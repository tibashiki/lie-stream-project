package com.an.job.udf;

import com.alibaba.fastjson.JSON;
import com.an.job.liveStreaming.pojo.DataBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JsonToDataBean extends ProcessFunction<String, DataBean> {
    @Override
    public void processElement(String string, ProcessFunction<String, DataBean>.Context context, Collector<DataBean> collector) throws Exception {
        try {
            DataBean dataBean = JSON.parseObject(string, DataBean.class);
            collector.collect(dataBean);
        }catch (Exception ignored){
            // TODO 解析失败
        }
    }
}
