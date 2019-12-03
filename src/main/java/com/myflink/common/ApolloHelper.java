package com.myflink.common;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.myflink.Distribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class ApolloHelper implements Serializable {

    enum condition{include,exclude}

    private Logger logger;
    private List<Tuple2<String,String>> configdata;

    public List<Tuple2<String, String>> getConfigdata() {
        return configdata;
    }

    public ApolloHelper(){
        logger=LoggerFactory.getLogger(ApolloHelper.class);
        configdata=new ArrayList<>();
    }

    public void ListenChange(Distribute dst,Config config,String clustername){
        //region 监听配置中心的配置，并重载配置
        config.addChangeListener(changeEvent -> {
            configInitial(config);
            dst.configs=Reset(configdata);

            for (String key : changeEvent.changedKeys()) {
                ConfigChange change = changeEvent.getChange(key);
                logger.info("{} Change - key: {}, oldValue: {}, newValue: {}, changeType: {}",
                        clustername,change.getPropertyName(), change.getOldValue(), change.getNewValue(),
                        change.getChangeType());
            }
        });
        //endregion
    }

    public void configInitial(Config config){
        configdata.clear();

        //region 路由规则配置
        String key_ADEP="ADEP";
        String defaultValue_ADEP="ZGGG";//default value if not set
        String value1 = config.getProperty(key_ADEP, defaultValue_ADEP);

        String key2="ADES";
        String defaultValue2="ZGHA";//default value if not set
        String value2 = config.getProperty(key2, defaultValue2);

        String key3="Company";
        String defaultValue3="CSN";//default value if not set
        String value3 = config.getProperty(key3, defaultValue3);

        String key4="ControlArea";
        String defaultValue4="ZGGGACC/ZGHAACC/ZHHHACC";//default value if not set
        String value4 = config.getProperty(key4, defaultValue4);

        String key5="StripState";
        String defaultValue5="";//default value if not set
        String value5 = config.getProperty(key5, defaultValue5);

        String key_adepflag="ADEP_Flag";
        String defaultValue_ADEP_FLAG=condition.include.toString();
        String value_ADEP_FLAG = config.getProperty(key_adepflag, defaultValue_ADEP_FLAG);

        String key_adesflag="ADES_Flag";
        String defaultValue_ADES_FLAG=condition.include.toString();
        String value_ADES_FLAG = config.getProperty(key_adesflag, defaultValue_ADES_FLAG);

        String key_cpyflag="Company_Flag";
        String defaultValue_cpy_FLAG=condition.include.toString();
        String value_cpy_FLAG = config.getProperty(key_cpyflag, defaultValue_cpy_FLAG);

        String key_CAflag="ControlArea_Flag";
        String defaultValue_CA_FLAG=condition.include.toString();
        String value_CA_FLAG = config.getProperty(key_CAflag, defaultValue_CA_FLAG);

        String key_STflag="StripState_Flag";
        String defaultValue_ST_FLAG=condition.exclude.toString();
        String value_ST_FLAG = config.getProperty(key_STflag, defaultValue_ST_FLAG);
        //endregion

        configdata.add(new Tuple2<>(value1, value_ADEP_FLAG));
        configdata.add(new Tuple2<>(value2, value_ADES_FLAG));
        configdata.add(new Tuple2<>(value3, value_cpy_FLAG));
        configdata.add(new Tuple2<>(value4, value_CA_FLAG));
        configdata.add(new Tuple2<>(value5, value_ST_FLAG));
    }

    public List<Tuple2<String[],String>> Reset(List<Tuple2<String,String>> strs){
        List<Tuple2<String[],String>> configs=new ArrayList<>();
        for (int i=0;i!=strs.size();i++){
            if(strs.get(i)._1.trim().isEmpty())
                configs.add(new Tuple2<>(new String[] {},strs.get(i)._2));
            else
                configs.add(new Tuple2<>(strs.get(i)._1.split(","),strs.get(i)._2));
        }
        return configs;
    }
}
