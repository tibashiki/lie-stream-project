package com.an.job.udf;

import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.util.log;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnel;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.Collections;
import java.util.Objects;

/**
 * CheckpointedFunction
 * 对重要的数据进行 存储到 状态后端
 */
public class IsNewUserFunction extends RichMapFunction<DataBean,DataBean> implements CheckpointedFunction {
    private transient ListState<BloomFilter<String>> listState;
    private transient BloomFilter<String> bloomFilter;

    /**
     * 初始化状态
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<BloomFilter<String>> stateDescriptor =
                new ListStateDescriptor<>(
                        "uid-bloom-filter-state",
                        TypeInformation.of(new TypeHint<BloomFilter<String>>() {})
                );
        listState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()){
            for (BloomFilter<String> bloomFilter : listState.get()) {
                this.bloomFilter = bloomFilter;
            }
        }
    }


    @Override
    public DataBean map(DataBean dataBean) throws Exception {
        String deviceId = dataBean.getDeviceId();

        if (Objects.isNull(bloomFilter)){
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(),10000);
        }
        if (!bloomFilter.mightContain(deviceId)){
            bloomFilter.put(deviceId);
            dataBean.setIsN(1);
        }
        return dataBean;
    }

    /**
     * 快照状态
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 把 bloomFilter 转换成 单例对象列表 存到 状态里面
        listState.update(Collections.singletonList(bloomFilter));
    }

}
