package cn.edu.zucc.traffic1.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/9 21:53
 * @desc:
 */
public class SpeedBean implements Writable {
    private String carId;
    private String direction;
    private Double position;
    private Long time;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(carId);
        dataOutput.writeUTF(direction);
        dataOutput.writeDouble(position);
        dataOutput.writeLong(time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.carId = dataInput.readUTF();
        this.direction = dataInput.readUTF();
        this.position = dataInput.readDouble();
        this.time = dataInput.readLong();
    }

    @Override
    public String toString() {
        return carId + "\t" + direction + "\t" + position + "\t" + time;
    }

    public SpeedBean(){

    }

    public SpeedBean(String carId, String direction, Double position, Long time) {
        this.carId = carId;
        this.direction = direction;
        this.position = position;
        this.time = time;
    }

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public Double getPosition() {
        return position;
    }

    public void setPosition(Double position) {
        this.position = position;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }
}
