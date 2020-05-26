package com.myflink.data;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadCastStream extends BroadcastProcessFunction<String, String, String> {
    private final MapStateDescriptor<String, String> mapStateDesc =
            new MapStateDescriptor<>(
                    "items",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);


    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(String s, Context context, Collector<String> collector) throws Exception {

    }
}
