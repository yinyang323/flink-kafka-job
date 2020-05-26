package com.myflink.data;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.calcite.shaded.org.apache.commons.codec.language.bm.Rule;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadCastStream extends BroadcastProcessFunction<String, Rule, String> {

    // store partial matches, i.e. first elements of the pair waiting for their second element
    // we keep a list as we may have many first elements waiting
    private final MapStateDescriptor<String, String> mapStateDesc =
            new MapStateDescriptor<>(
                    "items",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

    // identical to our ruleStateDescriptor above
    private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
            new MapStateDescriptor<>(
                    "RulesBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Rule>() {}));


    @Override
    public void processElement(String value, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(Rule value, Context ctx, Collector<String> collector) throws Exception {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.getPattern(), value);
    }
}
