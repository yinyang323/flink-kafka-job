package com.myflink.data;

import kong.unirest.Unirest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;


public class restfulSource extends RichParallelSourceFunction<String> {
    static private Boolean isRunning;
    static private String _url;
    static private String _topic;
    static private int _interval;
    static private String _groupid;
    static private String _host;
    static private int _port;
    static private String result;

    public restfulSource(String host,int port,String groupid,String topic,int interval){
        _host=host;
        _port=port;
        _groupid=groupid;
        _interval=interval;
        _topic=topic;
        _url="http://"+_host+":"+_port+"/ICE/consumer/"+_groupid+"/"+_topic;
        isRunning=true;
    }

    public restfulSource(String host,int port,String groupid,String topic){
        new restfulSource(host,port,groupid,topic,1000);
    }


    @Override
    public void run(SourceContext sourceContext) throws Exception {

        while (isRunning){
            Unirest.get(_url)
                    .asString()
                    .ifSuccess(response-> {
                        result=response.getBody();
                        result=result.replace("[\"","");
                        result=result.replace("\"]","");
                        result=result.replace("\\","");
                        if(result.contains(">")) {
                            sourceContext.collect(result);
                            //System.out.println(result);
                        }
                        else
                            System.out.println("Illegal result: "+result);
                    })
                    .ifFailure(response->{
                        result="";
                        System.out.println("Request source topic failed: "+response.getBody());
                        try {
                            Thread.sleep(_interval);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String url_create="http://"+_host+":"+_port+"/ICE/consumer/"+_topic+"/create";
        String response=Unirest.post(url_create)
                .header("accept","application/text")
                .queryString("groupid",_groupid)
                .queryString("num","1")
                .asString()
                .getBody();
        System.out.println(response);
    }
}
