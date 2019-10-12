package com.myflink.data;

import com.alibaba.fastjson.JSON;
import com.myflink.messages.record;
import kong.unirest.Unirest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class restfulSink extends RichSinkFunction<String> {

    static private String _url;
    static String _response;

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
        record _record=new record();
        _record.setKey("ICE processed");
        _record.setValue(value);
        _record.setPartition(0);

       Unirest.post(_url)
               .header("Content-Type","application/json")
               .body(JSON.toJSON(_record))
               .asString()
               .ifSuccess(response->{
                    _response=response.getBody();
                    //response res= JSON.parseObject(_response,response.class);
                    System.out.println(JSON.toJSON(_response));
               })
               .ifFailure(response->System.out.println(response.getBody()));
    }

}
