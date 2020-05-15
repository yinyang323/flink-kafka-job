package com.myflink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.dom4j.*;
import scala.Tuple2;
import sun.java2d.Disposer;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class Distribute extends Disposer implements Serializable {

    public List<Tuple2<String[], String>> configs = new ArrayList<>();
    private String[] xpaths = {};

    public void setXpaths(String[] xpaths) {
        this.xpaths = xpaths;
    }

    public Distribute(List<Tuple2<String, String>> strs) {

        for (int i = 0; i != strs.size(); i++) {
            if (strs.get(i)._1.trim().isEmpty())
                configs.add(new Tuple2<>(new String[]{}, strs.get(i)._2));
            else
                configs.add(new Tuple2<>(strs.get(i)._1.split(","), strs.get(i)._2));
        }
    }


    /*compare input and return tag num*/
    public boolean SelectTunnel(String input) throws Exception {

        String ADEP = strToXmltuple(input, xpaths[0], "locationIndicator");
        String ADES = strToXmltuple(input, xpaths[1], "locationIndicator");
        String Company = strToXmltuple(input, xpaths[2], "aircraftIdentification").substring(0, 3);
        String ControlArea = strToXmltuple(input, xpaths[3], "controlArea");

        String[] strings = {ADEP, ADES, Company, ControlArea};

        if (configs.size() == 0)
            return false;
        for (int i = 0; i != strings.length; i++) {
            if (!compareMessage(strings[i], configs.get(i)))
                return false;
        }
        return true;
    }

    private static String strToJSONObj(String jsonstr) {
        JSONObject jsonObject = JSON.parseObject(jsonstr);
        Object jsonarray = jsonObject.get("FlightDepInfos");

        String str = jsonarray + "";
        JSONArray array = JSON.parseArray(str);

        String outstr = "";
        //Collection<String> collection=new ArrayList<>();

        for (int i = 0; i < array.size(); i++) {
            JSONObject obj = JSON.parseObject(array.get(i) + "");
            outstr = outstr.concat(obj.getString("DepInfo") + ",");
            //collection.add(obj.getString("DepInfo"));
            //System.out.println(obj.get("name"));
        }
        return outstr;
    }

    private static String strToXmltuple(String xmlstr, String Xpath) throws DocumentException {
        Document document = DocumentHelper.parseText(xmlstr);
        Node type = document.selectSingleNode(Xpath);
        //Tuple2<String,String> tuple2=new Tuple2<>(msgtype,xmlstr);
        return type.getText();

    }

    private static String strToXmltuple(String xmlstr, String xpath, String attributeName) throws Exception {
       try {
           //SAXReader sr=new SAXReader();
           //Document document=sr.read(new ByteArrayInputStream(xmlstr.getBytes("utf-8")));
           Document document = DocumentHelper.parseText(xmlstr);
/*        Map<String, String> map = new HashMap<String, String>();
        map.put("xsd","http://www.w3.org/2001/XMLSchema");*/
           //XPath x=document.createXPath(xpath);
           Node type = document.selectSingleNode(xpath);
           StringBuilder str = new StringBuilder("@");
           str.append(attributeName);
           return type.valueOf(str.toString());
       }
       catch (Exception e){
           throw e;
       }
    }

    public Tuple5<String,String,String,String,String> convertToTuple(String input) throws Exception {
        try {
            String ADEP = strToXmltuple(input, xpaths[0], "locationIndicator");
            String ADES = strToXmltuple(input, xpaths[1], "locationIndicator");
            String Company = strToXmltuple(input, xpaths[2], "aircraftIdentification").substring(0, 3);
            String ControlArea = strToXmltuple(input, xpaths[3], "controlArea");

            return new Tuple5<>(ADEP,ADES,Company,ControlArea,input);
        }
        catch (Exception e){
            throw e;
        }
    }

    /*compare input message with config value*/
    private boolean compareMessage(String strings, Tuple2<String[], String> configs) throws DocumentException {
        switch (configs._2) {
            case "include":
                if (configs._1.length != 0) {
                    if (!(strings.trim().isEmpty()))
                        return isHave(configs._1, strings.trim());
                    else
                        return false;
                } else
                    return true;
            case "exclude":
                if (configs._1.length != 0) {
                    if (!(strings.trim().isEmpty())) {
                        return (!isHave(configs._1, strings.trim()));
                    } else
                        return false;
                } else
                    return true;
            default:
                return false;
        }
    }

    private boolean isHave(String[] strings, String str) {
        for (int i = 0; i != strings.length; i++) {
            if (strings[i].trim().equals(str))
                return true;
        }
        return false;
    }
}
