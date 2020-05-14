package com.myflink.messages;


public class response {

    private Integer partition=0;


    private long offset;


    private String error_code;


    private String error;

    public void setPartition(Integer partition){this.partition=partition;}
    public void setOffset(long offset){this.offset=offset;}
    public void setError_code(String code){this.error_code=code;}
    public void setError(String code){this.error=code;}

    public Integer getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getError_code() {
        return error_code;
    }

    public String getError() {
        return error;
    }
}