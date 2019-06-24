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
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.dom4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(StreamingJob.class);
    private Config config;
    private static Distribute distribute;

    public StreamingJob(){
        ConfigChangeListener changeListener = new ConfigChangeListener() {
            @Override
            public void onChange(ConfigChangeEvent changeEvent) {
                logger.info("Changes for namespace {}", changeEvent.getNamespace());
                for (String key : changeEvent.changedKeys()) {
                    ConfigChange change = changeEvent.getChange(key);
                    logger.info("Change - key: {}, oldValue: {}, newValue: {}, changeType: {}",
                            change.getPropertyName(), change.getOldValue(), change.getNewValue(),
                            change.getChangeType());
                }
            }
        };

        config = ConfigService.getAppConfig();
        config.addChangeListener(changeListener);

        String key1="key1";
        String defaultValue1="ZGGG,ZHHH,CSN,ZGGGACC/ZGHAACC/ZHHHACC";//default value if not set
        String value1=config.getProperty(key1,defaultValue1);

        String key2="key2";
        String defaultValue2="ZGGG,ZGHA,CSN,ZGGGACC/ZGHAACC";
        String value2=config.getProperty(key2,defaultValue2);

        String key3="key3";
        String defaultValue3="KDTW,EGLL,DAL,";
        String value3=config.getProperty(key3,defaultValue3);

        /*数据源所在的主题*/
        String SrcTopic="SrcTopic";
        String defaultValue4="test";
        String srcTopic=config.getProperty(SrcTopic,defaultValue4);

        String TarTopic1="TarTopic1";
        String defaultValue5="topic.quick.tran";
        String tarTopic1=config.getProperty(TarTopic1,defaultValue5);

        String TarTopic2="TarTopic2";
        String defaultValue6="topic.quick.ack";
        String tarTopic2=config.getProperty(TarTopic2,defaultValue6);

        String TarTopic3="TarTopic3";
        String defaultValue7="test111";
        String tarTopic3=config.getProperty(TarTopic3,defaultValue7);

        distribute=new Distribute();
        distribute.setSrcTopic(srcTopic);
        distribute.setTarTopic1(tarTopic1);
        distribute.setTarTopic2(tarTopic2);
        distribute.setTarTopic3(tarTopic3);
        distribute.setTunnels(new String[]{value1,value2,value3});

    }

    public static void main(String[] args) throws Exception {


        StreamingJob streamingJob=new StreamingJob();

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
				.addSource(new FlinkKafkaConsumer011<>(streamingJob.distribute.getSrcTopic(), new SimpleStringSchema(), prop1));

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
                    switch (distribute.SelectTunnel(s)) {
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
        dataStream1.addSink(new FlinkKafkaProducer011<>(streamingJob.distribute.getTarTopic1(), new SimpleStringSchema(),prop2));
        dataStream1.print();

        //DataStream<String> dataStream2=stringSplitStream.select(FlightPlan);
        DataStream<String> dataStream2=streamOperator.getSideOutput(outputTag2);
        dataStream2.addSink(new FlinkKafkaProducer011<>(streamingJob.distribute.getTarTopic2(), new SimpleStringSchema(),prop2));
        dataStream2.print();

        DataStream<String> dataStream3=streamOperator.getSideOutput(outputTag3);
        dataStream3.addSink(new FlinkKafkaProducer011<>(streamingJob.distribute.getTarTopic3(), new SimpleStringSchema(),prop2));
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


}


