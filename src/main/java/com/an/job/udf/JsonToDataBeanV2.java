package com.an.job.udf;

import com.alibaba.fastjson.JSON;
import com.an.job.liveStreaming.pojo.DataBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 作者：WQC
 * 项目：yjxxt-lzj
 * 公司：优极限学堂
 * 部门：大数据
 */
public class JsonToDataBeanV2 extends ProcessFunction<Tuple2<String,String>, DataBean> {
    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<DataBean> out) throws Exception {
        try {
            String id = value.f0;
            DataBean dataBean = JSON.parseObject(value.f1, DataBean.class);

            dataBean.setId(id);
            out.collect(dataBean);
        }catch (Exception e){
            //TODO 将异常数据保存到日志/测录输出
        }
    }
}
