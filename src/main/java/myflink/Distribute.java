package myflink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.dom4j.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class Distribute implements Serializable {
    private String srcTopic = "";
    private String tarTopic = "";
    private List<Tuple2<String,String>> tunnels=new ArrayList<>() ;
    private List<Tuple2<String[],String>> configs=new ArrayList<>();
    private String[] xpaths= {};

    public void setSrcTopic(String srcTopic) {
        this.srcTopic = srcTopic;
    }

    public void setTarTopic(String tarTopic) {
        this.tarTopic = tarTopic;
    }

    public void setXpaths(String[] xpaths){this.xpaths=xpaths;}


    public String getSrcTopic() { return srcTopic; }

    public String getTarTopic(){
        return tarTopic;
    }

    public Distribute(List<Tuple2<String,String>> strs){
        tunnels=strs;

        for (int i=0;i!=tunnels.size();i++){
            if(tunnels.get(i)._1.trim().isEmpty())
                configs.add(new Tuple2<>(new String[] {},tunnels.get(i)._2));
            else
                configs.add(new Tuple2<>(tunnels.get(i)._1.split(","),tunnels.get(i)._2));
        }
    }

    /*compare input and return tag num*/
    public boolean SelectTunnel(String input) throws DocumentException {
        String ADEP=strToXmltuple(input,xpaths[0],"locationIndicator");
        String ADES=strToXmltuple(input,xpaths[1],"locationIndicator");
        String Company=strToXmltuple(input,xpaths[2],"aircraftIdentification").substring(0,3);
        String ControlArea=strToXmltuple(input,xpaths[3],"controlArea");

        String[] strings={ADEP,ADES,Company,ControlArea};

        for(int i=0;i!=strings.length;i++){
            if(!compareMessage(strings[i], configs.get(i)))
                return false;
        }
        return true;
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

    private static String strToXmltuple(String xmlstr,String xpath,String attributeName) throws DocumentException {
        Document document=DocumentHelper.parseText(xmlstr);
/*        Map<String, String> map = new HashMap<String, String>();
        map.put("xsd","http://www.w3.org/2001/XMLSchema");*/
        //XPath x=document.createXPath(xpath);

        Node type= document.selectSingleNode(xpath);
        StringBuilder str=new StringBuilder("@");
        str.append(attributeName);
        return type.valueOf(str.toString());
    }

    /*compare input message with config value*/
    private boolean compareMessage(String strings,Tuple2<String[],String> configs) throws DocumentException {
        switch (configs._2) {
            case "include":
                if (configs._1.length != 0) {
                    if (!(strings.trim().isEmpty()))
                        return isHave(configs._1, strings.trim());
                    else
                        return false;
                }
                else
                    return true;
            case "exclude":
                if(configs._1.length!=0){
                    if(!(strings.trim().isEmpty())) {
                        return (!isHave(configs._1, strings.trim()));
                    }
                    else
                        return false;
                }
                else
                    return true;

                default: return false;
        }
    }

    private boolean isHave(String[] strings,String str){
        for(int i=0;i!=strings.length;i++){
            if(strings[i].trim().equals(str))
                return true;
        }
        return false;
    }
}
