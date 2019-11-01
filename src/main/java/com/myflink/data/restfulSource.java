package com.myflink.data;

import com.myflink.common.OkHttpHelper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


public class restfulSource extends RichSourceFunction<String> {
    private volatile Boolean isRunning;
    private static final long serialVersionUID=2L;

    private String _url;
    private String _topic;
    private String _groupid1;
    private String _host;
    private String url_create;
    //private String result;
    private int _port;


    public restfulSource(String host,int port,String group,String topic){
        _host=host;
        _port=port;
        _groupid1=group;
        _topic=topic;
        _url="http://"+_host+":"+_port+"/ICE/consumer/"+_groupid1+"/"+_topic;


        isRunning=true;

        url_create = "http://" + _host + ":" + _port + "/ICE/consumer/" + _topic + "/create";

    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {

        while (isRunning){

            Thread.sleep(100);

            OkHttpHelper.Comsume(_url,sourceContext);


        }


    }

    @Override
    public void cancel() {
        isRunning=false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        OkHttpHelper.createInstancce(_groupid1,url_create);

    }
}
