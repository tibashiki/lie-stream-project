package com.an.job.udf;

import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.util.log;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Objects;

import static com.an.job.constant.EventID.LIVE_LEAVE;


public class AnchorDistinctTotalAudienceFunc extends KeyedProcessFunction<String, DataBean, DataBean> {

    private transient ValueState<Integer> uvState;
    private transient ValueState<Integer> pvState;
    private transient ValueState<BloomFilter<String>> bloomFilterState;

    private transient ValueState<Integer> onlineState;
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyMMdd-HH");
    //TODO 案例 定义测流输出的标签
    private final OutputTag<Tuple4<String,Integer,Integer,Integer>> aggTag = new OutputTag("agg-tag",TypeInformation.of(new TypeHint<Tuple4<String,Integer,Integer,Integer>>() {})){};

    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO 案例  设置状态的 ttl
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        // TODO 案例 创建状态
        ValueStateDescriptor<Integer> uvStateDescriptor = new ValueStateDescriptor<>("uv-state", Integer.class);
        ValueStateDescriptor<Integer> pvStateDescriptor = new ValueStateDescriptor<>("pv-state", Integer.class);
        ValueStateDescriptor<Integer> onlineStateDescriptor = new ValueStateDescriptor<>("online-user-state", Integer.class);
        ValueStateDescriptor<BloomFilter<String>> bloomFilterStateDescriptor = new ValueStateDescriptor<>(
                "Bloom-filter-state",
                TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                })
        );

        // TODO 案例  给状态添加 TTL
        uvStateDescriptor.enableTimeToLive(ttlConfig);
        pvStateDescriptor.enableTimeToLive(ttlConfig);
        onlineStateDescriptor.enableTimeToLive(ttlConfig);
        bloomFilterStateDescriptor.enableTimeToLive(ttlConfig);
        // TODO 案例  获取状态的值
        uvState = getRuntimeContext().getState(uvStateDescriptor);
        pvState = getRuntimeContext().getState(pvStateDescriptor);
        onlineState = getRuntimeContext().getState(onlineStateDescriptor);
        bloomFilterState = getRuntimeContext().getState(bloomFilterStateDescriptor);
    }

    @Override
    public void processElement(
            DataBean bean,
            KeyedProcessFunction<String, DataBean, DataBean>.Context ctx,
            Collector<DataBean> out) throws Exception {

        // 获取 当前处理时间
        long processingTime = ctx.timerService().currentProcessingTime();
        long fireTime = processingTime - processingTime % (10 * 1000) + 10 * 1000;
        // TODO 设置 寄存器处理时间 用 onTimer 方法 执行
        ctx.timerService().registerProcessingTimeTimer(fireTime);

        //  pv  uv  在线人数逻辑
        String deviceId = bean.getDeviceId();
        Integer online = onlineState.value();
        if (Objects.isNull(online))
            online = 0;
        Integer uv = uvState.value();
        Integer pv = pvState.value();
        if (LIVE_LEAVE.equals(bean.getEventId())){

            BloomFilter<String> bloomFilter = bloomFilterState.value();

            if (Objects.isNull(bloomFilter)){
                bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(),10000);
                uv = 0;
                pv = 0;
            }

            if (!bloomFilter.mightContain(deviceId)){
                bloomFilter.put(deviceId);
                uv++;
                bloomFilterState.update(bloomFilter);
                uvState.update(uv);
            }

            pv++;
            pvState.update(pv);
            online++;
            onlineState.update(online);

        }else {
            online--;
            onlineState.update(online);
        }

        // 处理 详细数据
        String format = simpleDateFormat.format(bean.getTimestamp());
        String[] split = format.split("-");
        bean.setDate(split[0]);
        bean.setHour(split[1]);
        // 直接输出
        out.collect(bean);

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, DataBean, DataBean>.OnTimerContext ctx, Collector<DataBean> out) throws Exception {
        // 直接输出
        //        out.collect(
//                Tuple4.of(
//                        ctx.getCurrentKey(),
//                        uvState.value(),
//                        pvState.value(),
//                        onlineState.value()
//                )
//        );
        String day = simpleDateFormat.format(timestamp).split("-")[0];
        // TODO 侧输出 在 aggTag 输出 内容
        ctx.output(
                aggTag,
                Tuple4.of(
                        // 做为redis 的 key
                        ctx.getCurrentKey()+"-"+day,
                        uvState.value(),
                        pvState.value(),
                        onlineState.value()
                )
        );
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 获取 侧输出流，给一个 get方法
     * @return <Tuple4<String, Integer, Integer, Integer>
     */
    public OutputTag<Tuple4<String, Integer, Integer, Integer>> getAggTag() {
        return aggTag;
    }
}
