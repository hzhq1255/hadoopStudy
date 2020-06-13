package cn.edu.zucc.traffic1.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/9 23:01
 * @desc:
 */
public class SpeedResult implements WritableComparable<SpeedResult> {
    private String hour;
    private Double speed;
    private String position1;
    private String position2;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(hour);
        dataOutput.writeDouble(speed);
        dataOutput.writeUTF(position1);
        dataOutput.writeUTF(position2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hour = dataInput.readUTF();
        this.speed = dataInput.readDouble();
        this.position1 = dataInput.readUTF();
        this.position2 = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return hour + "\t" + speed + "\t" + position1 + "\t" + position2;
    }

    @Override
    public int compareTo(SpeedResult o) {
        return hour.compareTo(o.hour);
    }
    public SpeedResult(String hour, Double speed, String position1, String position2) {
        this.hour = hour;
        this.speed = speed;
        this.position1 = position1;
        this.position2 = position2;
    }

    public SpeedResult() {
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(Double speed) {
        this.speed = speed;
    }

    public String getPosition1() {
        return position1;
    }

    public void setPosition1(String position1) {
        this.position1 = position1;
    }

    public String getPosition2() {
        return position2;
    }

    public void setPosition2(String position2) {
        this.position2 = position2;
    }
}
