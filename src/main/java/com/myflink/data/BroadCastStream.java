package com.myflink.data;


import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.LocalDateTime;
import java.util.Set;

public class BroadCastStream extends RichParallelSourceFunction<String> {

    private volatile boolean isRun;
    private volatile int lastUpdateMin = -1;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRun = true;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRun) {
            LocalDateTime date = LocalDateTime.now();
            int min = date.getMinute();
            if (min != lastUpdateMin) {
                lastUpdateMin = min;
                Set<String> configs = readConfigs();
                if (configs != null && configs.size() > 0) {
                    for (String config : configs) {
                        ctx.collect(config);
                    }

                }
            }
            Thread.sleep(1000);
        }
    }

    private Set<String> readConfigs() {
        //这里读取配置信息
        Config config = ConfigService.getAppConfig();

        return null;
    }


    @Override
    public void cancel() {
        isRun = false;
    }
}
