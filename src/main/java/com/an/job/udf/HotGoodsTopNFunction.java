package com.an.job.udf;

import com.an.job.liveStreaming.pojo.ItemEventCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HotGoodsTopNFunction extends KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount , List<ItemEventCount>> {
    private transient ValueState<List<ItemEventCount>> listValueState;
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<List<ItemEventCount>> listValueStateDescriptor = new ValueStateDescriptor<>("ItemEventList-state", TypeInformation.of(new TypeHint<List<ItemEventCount>>() {
        }));
        listValueState = getRuntimeContext().getState(listValueStateDescriptor);
    }

    @Override
    public void processElement(ItemEventCount value, KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount, List<ItemEventCount>>.Context ctx, Collector<List<ItemEventCount>> out) throws Exception {
        List<ItemEventCount> itemEventCountList = listValueState.value();
        if (Objects.isNull(itemEventCountList)){
            itemEventCountList = new ArrayList<>();
        }
        itemEventCountList.add(value);
        listValueState.update(itemEventCountList);
        ctx.timerService().registerProcessingTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple4<String, String, Long, Long>, ItemEventCount, List<ItemEventCount>>.OnTimerContext ctx, Collector<List<ItemEventCount>> out) throws Exception {
        List<ItemEventCount> itemEventCountList = listValueState.value();
        itemEventCountList.sort((a,b) -> Long.compare(b.count,a.count));
        List<ItemEventCount> list = new ArrayList<>();
        for (int i = 0; i < Math.min(3,itemEventCountList.size()); i++) {
            list.add(itemEventCountList.get(i));
        }
        out.collect(list);

    }
}
