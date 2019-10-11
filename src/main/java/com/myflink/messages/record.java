package com.myflink.messages;

public class record {
    private String key=null;

    private String value=null;

    private int partition=0;

    public void setKey(String key){this.key=key;}
    public String getKey(){return key;}

    public void setValue(String value){this.value=value;}
    public String getValue(){return value;}

    public void setPartition(int partition){this.partition=partition;}
    public int getPartition(){return partition;}
}
