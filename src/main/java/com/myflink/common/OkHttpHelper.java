package com.myflink.common;

import com.alibaba.fastjson.JSON;
import com.myflink.messages.record;
import okhttp3.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OkHttpHelper {
    static private OkHttpClient _client=new OkHttpClient();
    static private Logger logger=LoggerFactory.getLogger(OkHttpHelper.class);

    static public void createInstancce(String _groupid1,String url_create){

        RequestBody body = new FormBody.Builder()
                .add("groupid", _groupid1)
                .add("num", "1")
                .build();

        Request request1 = new Request.Builder()
                .url(url_create)
                .post(body)
                .build();

        _client.newCall(request1).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                logger.error(call.toString());
                logger.error(e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (response.isSuccessful()) {
                    logger.info(response.body().string());
                }
                response.close();
            }
        });

    }

    static public void Comsume(String _url, SourceFunction.SourceContext sc) {

        Request request = new Request.Builder()
                .url(_url)
                .get()
                .build();
        try {
            //region 异步发送消费请求
        /*_client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                System.out.println(call.toString());
                System.out.println(e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (response.isSuccessful()) {
                    String _result = response.body().string();
                    _result = _result.replace("[\"", "");
                    _result = _result.replace("\"]", "");
                    _result = _result.replace("\\", "");
                    if (_result.contains(">")) {
                        // this synchronized block ensures that state checkpointing,
                        // internal state updates and emission of elements are an atomic operation
                        //synchronized (sourceContext.getCheckpointLock()) {
                        //sourceContext.collect(result);
                        // }
                        sc.collect(_result);
                    } else {
                        //sourceContext.markAsTemporarilyIdle();
                        System.out.println("Illegal result: " + _result);
                    }
                }
                response.close();
            }
        });*/
        //endregion

            //region 同步发送消费请求
                Response response = _client.newCall(request).execute();
                String _result = response.body().string();
                _result = _result.replace("[\"", "");
                _result = _result.replace("\"]", "");
                _result = _result.replace("\\", "");
                if (_result.contains(">")) {
                    sc.collect(_result);
                    logger.debug("Receive message:");
                    logger.debug(_result);
                }
                else if(_result.equals("[]"))
                    {}
                else {
                    logger.info("Illegal result: " + _result);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            //endregion


    }

        public static void product(String _url,String value){
        RequestBody body=RequestBody
                .create(MediaType.parse("application/json;charset=utf-8"),JSON.toJSON(toObj(value)).toString());
        Request request = new Request.Builder()
                .url(_url)
                .post(body)
                .build();

        _client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                logger.error("fail: "+call.toString());
                logger.error(e.getMessage());
            }
            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (response.isSuccessful()) {
                    logger.info("Send message success,content: "+response.body().string());
                }
                response.close();
            }
        });
    }

    private static record toObj(String value){
        record _record=new record();
        _record.setKey("ICE processed");
        _record.setValue(value);
        _record.setPartition(0);

        return _record;
    }



}
