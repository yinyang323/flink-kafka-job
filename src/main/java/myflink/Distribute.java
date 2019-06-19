package myflink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

public class Distribute {
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

    public String getValue1() {
        return value1;
    }

    public String getValue2() {
        return value2;
    }

    public String getValue3() {
        return value3;
    }

    public String getSrcTopic() {
        return srcTopic;
    }

    public String getTarTopic1() {
        return tarTopic1;
    }

    public String getTarTopic2() {
        return tarTopic2;
    }

    public String getTarTopic3() {
        return tarTopic3;
    }

    /*compare input and return tag num*/
    public int SelectTunnel(String input) throws DocumentException {
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
