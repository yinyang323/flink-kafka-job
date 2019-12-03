/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.myflink;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.myflink.common.ApolloHelper;
import com.myflink.data.restfulSink;
import com.myflink.data.restfulSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.dom4j.*;

import javax.net.ssl.*;
import java.net.URL;
import java.security.cert.X509Certificate;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {



    //static public Distribute distribute;

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        /*指定apollo中所在的集群名称*/
        String clustername=parameterTool.get("apollo.cluster","default");
        System.setProperty("apollo.cluster",clustername);
        Config config=ConfigService.getAppConfig();
        //final Logger logger = LoggerFactory.getLogger(StreamingJob.class);

        //String namespace=parameterTool.get("instance.name","instance1");

        //CommonConfig=ConfigService.getAppConfig();


        /*cancel ssl secure*/
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {  }
                }
        };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());

        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        HttpsURLConnection.setDefaultHostnameVerifier((s, sslSession) -> true);

        //region 基本配置
        String xpath="xpath";
        String xpathdefaultValue="//mesg:Message/mesg:flight/fx:departure/fx:aerodrome,//mesg:Message/mesg:flight/fx:arrival/fx:destinationAerodrome," +
                "//mesg:Message/mesg:flight/fx:flightIdentification,//mesg:Message/mesg:flight/fb:extension/atmb:atmbFipsInfo";
        String xpathvalue=config.getProperty(xpath,xpathdefaultValue);

        /*数据源所在的主题*/
        String SrcTopic="Source";
        String SrcdefaultValue="source1";
        String srcTopic = config.getProperty(SrcTopic, SrcdefaultValue);

        String TarTopic="Target";
        String TardefaultValue="output1";
        String tarTopic = config.getProperty(TarTopic, TardefaultValue);

        /*公共配置项*/
        String Recv="recv.server";
        String defaultValue8="http://192.168.191.131:8081";
        URL recv=new URL(config.getProperty(Recv,defaultValue8));

        String Send="send.server";
        String defaultValue9="http://192.168.191.131:8081";
        URL send=new URL(config.getProperty(Send,defaultValue9));
        //endregion

        ApolloHelper apollo=new ApolloHelper();
        apollo.configInitial(config);

        Distribute distribute=new Distribute(apollo.getConfigdata());
        distribute.setXpaths(xpathvalue.split(","));

        apollo.ListenChange(distribute,config);

        final OutputTag<String> outputTag1 = new OutputTag<String>("output1"){};

        /*此配置若不设置默认值，则可能根据当前机器的cpu线程数设置并发数*/
        StreamExecutionEnvironment.setDefaultLocalParallelism(1);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);


        restfulSource source = new restfulSource(recv.getHost(),recv.getPort(),"group.id-"+clustername, srcTopic);
        restfulSink tar1 = new restfulSink(send.getHost(),send.getPort(), tarTopic);

        /*将消费模式设置为从broker记录的位置开始，防止消息丢失*/
        /*setStartFromEarliest() /setStartFromLatest(): 即从最早的/最新的消息开始消费*/
		DataStreamSource stream = env
				.addSource(source);

//        SplitStream<String> stringSplitStream = stream.split(
//                new OutputSelector<String>() {
//                    @Override
//                    public Iterable<String> select(String s) {
//                        List<String> output=new ArrayList<>();
//                        try {
//                            String key=strToXmltuple(s);
//                            switch (key){
//                                case FlightDepInfo:
//                                    output.add(FlightDepInfo);
//                                    break;
//                                case FlightPlan:
//                                    output.add(FlightPlan);
//                                    break;
//                                    default:
//                                        break;
//                            }
//                        } catch (DocumentException e) {
//                            e.printStackTrace();
//                        }
//                        return output;
//                    }
//                }
//        );

        SingleOutputStreamOperator streamOperator= stream.process(new ProcessFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.setProperty("apollo.cluster",clustername);
                Config _config=ConfigService.getAppConfig();
                apollo.ListenChange(distribute,_config);
            }

            @Override
            public void processElement(String s, Context context, Collector<String> out) throws Exception {
                out.collect(s);

                try {
//                    switch (distribute.SelectTunnel(s)) {
//                        case 0:
//                            context.output(outputTag1, s);
//                            break;
//                        default:
//                            break;
                    if(distribute.SelectTunnel(s)){
                        context.output(outputTag1,s);
                    }
                }
                catch (DocumentException e) {
                    e.printStackTrace();
                }
                catch (Exception ex){
                    throw ex;
                }

            }


        }).setParallelism(1);


        //DataStream<String> dataStream1=stringSplitStream.select(FlightDepInfo);
        DataStream dataStream1=streamOperator.getSideOutput(outputTag1);
        dataStream1.addSink(tar1);
        dataStream1.print();



//        KeyedStream<String,Tuple> keyedStream1= (Datastream.map(new MapFunction<Tuple2<String,String>, String>() {
////            private static final long serialVersionUID = -6867736771747690201L;
////
////            @Override
////            public String map(Tuple2<String,String> tuple2) throws Exception {
////                return tuple2._2;
////            }
////        }).keyBy(new KeySelector<String, Tuple>() {
////            @Override
////            public Tuple getKey(String s) throws Exception {
////                return null;
////            }
////        }));

//        stream.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out)
//            throws Exception {
//                out = strToJSONObj(value);
//            }
//        });




		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("数据交换平台智能路由"+clustername);

	}



}


