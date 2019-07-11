package myflink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

import java.io.Serializable;

public class Distribute implements Serializable {
    private String srcTopic = "";
    private String tarTopic = "";

    private String[] tunnels = {};

    public void setSrcTopic(String srcTopic) {
        this.srcTopic = srcTopic;
    }

    public void setTarTopic(String tarTopic) {
        this.tarTopic = tarTopic;
    }

    public void setTunnels(String[] tunnels) {
        this.tunnels = tunnels;
    }



    public String getSrcTopic() {

        return srcTopic;
    }

    public String getTarTopic(){
        return tarTopic;
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

    private static String strToXmltuple(String xmlstr,String Xpath,String attributeName) throws DocumentException {
        Document document=DocumentHelper.parseText(xmlstr);
        Node type= document.selectSingleNode(Xpath);
        StringBuilder str=new StringBuilder("@");
        str.append(attributeName);
        return type.valueOf(str.toString());
    }

    /*compare input message with config value*/
    private boolean compareMessage(String input,String config) throws DocumentException {

        String ADEP=strToXmltuple(input,"//mesg:Message/mesg:flight/fx:departure/fx:aerodrome","locationIndicator");
        String ADES=strToXmltuple(input,"//mesg:Message/mesg:flight/fx:arrival/fx:destinationAerodrome","locationIndicator");
        String Company=strToXmltuple(input,"//mesg:Message/mesg:flight/fx:flightIdentification","aircraftIdentification").substring(0,3);
        String ControlArea=strToXmltuple(input,"//mesg:Message/mesg:flight/fb:extension/atmb:atmbFipsInfo","controlArea");

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
