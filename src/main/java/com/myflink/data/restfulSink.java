package com.myflink.data;

import com.alibaba.fastjson.JSON;
import com.myflink.StreamingJob;
import com.myflink.common.OkHttpHelper;
import com.myflink.messages.record;
import okhttp3.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;


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


     /*   String body=JSON.toJSON(_record).toString();
        _response=conn.post(_url,body);*/
        //System.out.println(_response);


        /*Request request = new Request.Builder()
                .url(_url)
                .post(RequestBody.create(MediaType
                        .parse("application/json;charset=utf-8"),JSON.toJSON(tojson(value)).toString()))
                .build();

        _client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                System.out.println("fail: "+call.toString());
                System.out.println(e.getMessage());
            }
            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (response.isSuccessful()) {
                    _response= response.body().string();
                }
                System.out.println(_response);
                response.close();
            }
        });*/

        OkHttpHelper.product(_url,value);
    }



       /* _response=Unirest.post(_url)
               .header("Content-Type","application/json")
               .body(JSON.toJSON(_record).toString())
               .asString()
               .getBody();

        //response res= JSON.parseObject(_response,response.class);
        System.out.println(JSON.toJSON(_response));*/

              /* .ifSuccess(response->{
                    _response=response.getBody();
                    //response res= JSON.parseObject(_response,response.class);
                    System.out.println(JSON.toJSON(_response));
               })
               .ifFailure(response->System.out.println(response.getBody()));*/
    }


