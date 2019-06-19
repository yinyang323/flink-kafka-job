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

package myflink;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.dom4j.*;


import java.util.*;

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

    Config config = ConfigService.getAppConfig();

    String key1="key1";
    String defaultValue1="ZGGG,ZHHH,CSN,ZGGGACC/ZGHAACC/ZHHHACC";//default value if not set
    String value1=config.getProperty(key1,defaultValue1);

    String key2="key2";
    String defaultValue2="ZGGG,ZGHA,CSN,ZGGGACC/ZGHAACC";
    String value2=config.getProperty(key2,defaultValue2);

    String key3="key2";
    String defaultValue3="ZGGG,ZHCC,CSN,ZGGGACC/ZGHAACC/ZHCCACC/ZHHHACC";
    String value3=config.getProperty(key2,defaultValue3);

    String[] tunnels={value1,value2,value3};

    String SrcTopic="SrcTopic";
    String defaultValue4="fixm";
    String srcTopic=config.getProperty(SrcTopic,defaultValue4);

    String TarTopic1="TarTopic1";
    String defaultValue5="zgha_fimx";
    String tarTopic1=config.getProperty(TarTopic1,defaultValue5);

    String TarTopic2="TarTopic2";
    String defaultValue6="zhhh_fimx";
    String tarTopic2=config.getProperty(TarTopic2,defaultValue6);

    String TarTopic3="TarTopic3";
    String defaultValue7="zhcc_fimx";
    String tarTopic3=config.getProperty(TarTopic3,defaultValue7);





    public void main(String[] args) throws Exception {


        final OutputTag<String> outputTag1 = new OutputTag<String>("output1"){};
        final OutputTag<String> outputTag2 = new OutputTag<String>("output2"){};
		final OutputTag<String> outputTag3 = new OutputTag<String>("output3"){};

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		Properties prop1 = new Properties();
		prop1.setProperty("bootstrap.servers", parameterTool.getRequired("recv.servers"));
		prop1.setProperty("group.id", "flink_consumer");

        Properties prop2 = new Properties();
        prop2.setProperty("bootstrap.servers", parameterTool.getRequired("send.servers"));

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer011<>(srcTopic, new SimpleStringSchema(), prop1));

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

        SingleOutputStreamOperator<String> streamOperator= stream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> out) throws Exception {
                out.collect(s);


                try {
                    switch (SelectTunnel(s)) {
                        case 0:
                            context.output(outputTag1, s);
                            break;
                        case 1:
                            context.output(outputTag2, s);
                            break;
                        case 2:
                            context.output(outputTag3, s);
                            break;
                        default:
                            break;
                    }
                }
                catch (DocumentException e) {
                    e.printStackTrace();
                }
                catch (Exception ex){
                    throw ex;
                }

            }

        });

        //DataStream<String> dataStream1=stringSplitStream.select(FlightDepInfo);
        DataStream<String> dataStream1=streamOperator.getSideOutput(outputTag1);
        dataStream1.addSink(new FlinkKafkaProducer011<>(tarTopic1, new SimpleStringSchema(),prop2));
        dataStream1.print();

        //DataStream<String> dataStream2=stringSplitStream.select(FlightPlan);
        DataStream<String> dataStream2=streamOperator.getSideOutput(outputTag2);
        dataStream2.addSink(new FlinkKafkaProducer011<>(tarTopic2, new SimpleStringSchema(),prop2));
        dataStream2.print();

        DataStream<String> dataStream3=streamOperator.getSideOutput(outputTag3);
        dataStream3.addSink(new FlinkKafkaProducer011<>(tarTopic3, new SimpleStringSchema(),prop2));
        dataStream3.print();


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
		env.execute("Flink Streaming Java API Skeleton");
	}

	/*compare input and return tag num*/
	private int SelectTunnel(String input) throws DocumentException {
        for(int i=0;i!=tunnels.length;i++){
            if(compareMessage(input,tunnels[i]))
                return i;
        }
        return -1;
    }

	private static String strToJSONObj(String jsonstr){
		JSONObject jsonObject=JSON.parseObject(jsonstr);
		Object jsonarray = jsonObject.get("FlightDepInfos");

		String str=	jsonarray+"";
		JSONArray array=JSON.parseArray(str);

		String outstr="";
        //Collection<String> collection=new ArrayList<>();

		for (int i = 0; i < array.size(); i++) {
			JSONObject obj = JSON.parseObject(array.get(i)+"");
			outstr=outstr.concat(obj.getString("DepInfo")+",");
            //collection.add(obj.getString("DepInfo"));
			//System.out.println(obj.get("name"));
			}
			return outstr;
	}

	private static String strToXmltuple(String xmlstr,String Xpath) throws DocumentException {
		Document document=DocumentHelper.parseText(xmlstr);
		Node type= document.selectSingleNode(Xpath);
		String msgtype=type.getText();
        //Tuple2<String,String> tuple2=new Tuple2<>(msgtype,xmlstr);
        return  msgtype;

	}

	/*compare input message with config value*/
	private boolean compareMessage(String input, String config) throws DocumentException {

        String ADEP=strToXmltuple(input,"");
        String ADES=strToXmltuple(input,"");
        String Company=strToXmltuple(input,"");
        String ControlArea=strToXmltuple(input,"");

        String[] strings={ADEP,ADES,Company,ControlArea};
        String[] configs=config.split(",");

        for(int i=0;i!=configs.length;i++){
            if(strings[i].isEmpty())
                continue;

            if(!strings[i].equals(configs[i]))
                return false;
        }
        return true;
    }


}


