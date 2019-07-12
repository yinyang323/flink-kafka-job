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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
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

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        /*指定apollo中所在的集群名称*/
        System.setProperty("apollo.cluster",parameterTool.get("apollo.cluster","DefaultCluster"));
        //final Logger logger = LoggerFactory.getLogger(StreamingJob.class);

        Config config,CommonConfig;
        Distribute distribute=new Distribute();

        config = ConfigService.getAppConfig();
        CommonConfig=ConfigService.getConfig("CE.brokers");


        String key1="condition";
        String defaultValue1="ZGGG,ZHHH,CSN,ZGGGACC/ZGHAACC/ZHHHACC";//default value if not set
        String value1=config.getProperty(key1,defaultValue1);


        /*数据源所在的主题*/
        String SrcTopic="SrcTopic";
        String defaultValue4="test";

        String srcTopic=config.getProperty(SrcTopic,defaultValue4);

        String TarTopic1="target";
        String defaultValue7="test111";
        String tarTopic1=config.getProperty(TarTopic1,defaultValue7);

        distribute.setSrcTopic(srcTopic);
        distribute.setTarTopic(tarTopic1);
        distribute.setTunnels(new String[]{value1});

        /*公共配置项*/
        String Recv="recv.server";
        String defaultValue8="192.168.136.132:9092";
        String recv=CommonConfig.getProperty(Recv,defaultValue8);

        String Send="send.server";
        String defaultValue9="192.168.136.132:9092";
        String send=CommonConfig.getProperty(Send,defaultValue9);

        final OutputTag<String> outputTag1 = new OutputTag<String>("output1"){};

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties prop1 = new Properties();
		prop1.setProperty("bootstrap.servers", recv);
		/*防止消费组名重复*/
		prop1.setProperty("group.id", parameterTool.get("apollo.cluster","DefaultCluster"));

        Properties prop2 = new Properties();
        prop2.setProperty("bootstrap.servers", send);

        FlinkKafkaConsumer011 source = new FlinkKafkaConsumer011<>(distribute.getSrcTopic(), new org.apache.flink.api.common.serialization.SimpleStringSchema(), prop1);
        FlinkKafkaProducer011 tar1 = new FlinkKafkaProducer011<>(distribute.getTarTopic(), new org.apache.flink.api.common.serialization.SimpleStringSchema(), prop2);

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
            public void processElement(String s, Context context, Collector<String> out) throws Exception {
                out.collect(s);

                try {
                    switch (distribute.SelectTunnel(s)) {
                        case 0:
                            context.output(outputTag1, s);
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
		env.execute("数据交换平台智能路由");

//        ConfigChangeListener changeListener = new ConfigChangeListener() {
//            @Override
//            public void onChange(ConfigChangeEvent changeEvent) {
//                logger.info("Changes for namespace {}", changeEvent.getNamespace());
//                for (String key : changeEvent.changedKeys()) {
//                    ConfigChange change = changeEvent.getChange(key);
//                    logger.info("Change - key: {}, oldValue: {}, newValue: {}, changeType: {}",
//                            change.getPropertyName(), change.getOldValue(), change.getNewValue(),
//                            change.getChangeType());
//                    switch (change.getPropertyName()){
//                        case "SrcTopic":
//                            try {
//                                source.cancel();
//                                source.close();
//
////                                KafkaTopicPartition ktp =new KafkaTopicPartition(change.getNewValue(),0);
////                                Map<KafkaTopicPartition,Long> start=new HashMap<>();
////                                start.put(ktp,0L);
////                                source.setStartFromSpecificOffsets(start);
////                                source.setStartFromLatest();/*only receive latest message*/
////                                source.open(null);
//                                source=new FlinkKafkaConsumer011(change.getNewValue(),new org.apache.flink.api.common.serialization.SimpleStringSchema(),prop1);
//                                env.addSource(source);
//
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                            break;
//
//                        case  "TarTopic1":
//                            try {
//                                tar1.close();
//                                tar1=new FlinkKafkaProducer011(change.getNewValue(), new org.apache.flink.api.common.serialization.SimpleStringSchema(),prop2);
//                                dataStream1.addSink(tar1);
//                            } catch (FlinkKafka011Exception e) {
//                                e.printStackTrace();
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                            break;
//
//                        case  "TarTopic2":
//                            try {
//                                tar2.close();
//                                tar2=new FlinkKafkaProducer011(change.getNewValue(),new org.apache.flink.api.common.serialization.SimpleStringSchema(),prop2);
//                                dataStream2.addSink(tar2);
//                            } catch (FlinkKafka011Exception e) {
//                                e.printStackTrace();
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                            break;
//
//                        case  "TarTopic3":
//                            try {
//                                tar3.close();
//                                tar3=new FlinkKafkaProducer011(change.getNewValue(), new org.apache.flink.api.common.serialization.SimpleStringSchema(),prop2);
//                                dataStream3.addSink(tar3);
//                            } catch (FlinkKafka011Exception e) {
//                                e.printStackTrace();
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                            break;
//
//                        default:
//                            break;
//                    }
//                }
//            }
//        };

        //config.addChangeListener(changeListener);

	}


}


