package com.belleintl.finkbean;


import jdk.nashorn.internal.objects.annotations.Getter;
import jdk.nashorn.internal.objects.annotations.Setter;

/**
 * @ClassName: SensorReading
 * @Description: SensorReading样例类
 * @Author: zhipengl01
 * @Date: 2021/9/3
 */
public class SensorReading {

    private String id;
    private Long timestamp;
    private Double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
      this.id = id;
      this.timestamp = timestamp;
      this.temperature = temperature;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getId(){
      return id;
    }

    public void setTimestamp(Long timestamp) {
      this.timestamp = timestamp;
    }

    public Long getTimestamp() {
      return timestamp;
    }

    public void setTemperature(Double temperature) {
      this.temperature = temperature;
    }

    public Double getTemperature() {
      return temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
