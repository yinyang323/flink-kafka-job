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


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.Collector;


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
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		Properties prop1 = new Properties();
		prop1.setProperty("bootstrap.servers", parameterTool.getRequired("recv.servers"));
		prop1.setProperty("group.id", "flink_consumer");

        Properties prop2 = new Properties();
        prop2.setProperty("bootstrap.servers", parameterTool.getRequired("send.servers"));

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), prop1));

        DataStream<String> Datastream=stream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
            	if(value==null||value.isEmpty())
            		return null;
            	else
                	return strToJSONObj(value);
            }

        });

//        stream.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out)
//            throws Exception {
//                out = strToJSONObj(value);
//            }
//        });

        Datastream.addSink(new FlinkKafkaProducer011<>("topic.quick.tran", new SimpleStringSchema(),prop2));


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




}


