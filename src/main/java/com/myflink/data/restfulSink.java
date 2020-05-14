package com.myflink.data;

import com.myflink.common.OkHttpHelper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;



public class restfulSink extends RichSinkFunction<String> {

    private static final long serialVersionUID=1L;
    private String _url;

    public restfulSink(String host,int port,String topic){
        _url="http://"+host+":"+port+"/ICE/producer/groupid/"+topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
    }

    @Override
    public void close()throws Exception{
        super.close();
    }

    @Override
    public void invoke (String value,Context context)throws Exception{

        OkHttpHelper.product(_url,value);
    }

    }


