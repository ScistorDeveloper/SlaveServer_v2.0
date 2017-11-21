package com.scistor.process.pojo;

/**
 * Created by WANG Shenghua on 2017/10/27.
 */
public class DataInfo {

    private String HOST;
    private int COUNT;
    private String STATUS;

    public DataInfo(String HOST, int COUNT, String STATUS) {
        this.HOST = HOST;
        this.COUNT = COUNT;
        this.STATUS = STATUS;
    }

    public String getHOST() {
        return HOST;
    }

    public void setHOST(String HOST) {
        this.HOST = HOST;
    }

    public int getCOUNT() {
        return COUNT;
    }

    public void setCOUNT(int COUNT) {
        this.COUNT = COUNT;
    }

    public String getSTATUS() {
        return STATUS;
    }

    public void setSTATUS(String STATUS) {
        this.STATUS = STATUS;
    }

    @Override
    public String toString() {
        return "DataInfo{" +
                "HOST='" + HOST + '\'' +
                ", COUNT=" + COUNT +
                ", STATUS='" + STATUS + '\'' +
                '}';
    }

}
