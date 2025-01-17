package org.kafkaApp.Structure.entities;

public class DataStructure {
    private String streamID;
    private String objectID;
    private String dataSetKey;
    private String date;
    private String time;
    private double price;
    private int volume;

    public DataStructure() {}

    public DataStructure(String streamID, String dataSetKey, String date, String time, double price, int volume) {
        this.streamID = streamID;
        this.dataSetKey = dataSetKey;
        this.date = date;
        this.time = time;
        this.price = price;
        this.volume = volume;
    }

    public DataStructure(String streamID, String objectID, String dataSetKey, String date, String time, double price, int volume) {
        this.dataSetKey = dataSetKey;
        this.date = date;
        this.time = time;
        this.price = price;
        this.volume = volume;
        this.streamID = streamID;
        this.objectID = objectID;
    }

    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }


    public String getDataSetKey() {
        return dataSetKey;
    }

    public void setDataSetKey(String dataSetKey) {
        this.dataSetKey = dataSetKey;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    public String getObjectID() {
        return objectID;
    }

    public void setObjectID(String objectID) {
        this.objectID = objectID;
    }
    /*@Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DataStructure that = (DataStructure) obj;
        return Double.compare(that.price, price) == 0 &&
                volume == that.volume &&
                streamID.equals(that.streamID) &&
                objectID.equals(that.objectID) &&
                dataSetKey.equals(that.dataSetKey) &&
                date.equals(that.date) &&
                time.equals(that.time);
    }*/
   /* @Override
    public String toString() {
        return "{" + "objectID=" + objectID + ", dataSetKey=" + dataSetKey + ", date=" + date + ", price=" + price
                + ", streamID=" + streamID + ", time=" + time + ", volume=" + volume + "}";
    }*/
}
