package com.an.job.udf;

import com.an.job.liveStreaming.pojo.DataBean;
import org.apache.flink.api.common.functions.AggregateFunction;

public class HotGoodsAggregateFunction implements AggregateFunction<DataBean, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(DataBean value, Long accumulator) {
        return accumulator+1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
