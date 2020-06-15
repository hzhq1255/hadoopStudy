package cn.edu.zucc.traffic3.knn;

import com.google.inject.internal.asm.$AnnotationVisitor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/13 22:32
 * @desc:
 */
public class DataBean implements Writable {

    private double flow;
    private double speed;
    private int[] positions;
    private int[] times;


    public DataBean(){

    }

    public DataBean(String str){
        String[] line = str.split("\t");
        this.flow = Double.parseDouble(line[0]);
        this.speed = Double.parseDouble(line[1]);
        this.positions = stringToArray(line[2]);
        this.times = stringToArray(line[3]);
    }

    public DataBean(double flow, double speed, int[] positions, int[] times) {
        this.flow = flow;
        this.speed = speed;
        this.positions = positions;
        this.times = times;
    }

    private String arrayToString(int[] array){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < array.length; i++){
            if (i == array.length - 1){
                builder.append(array[i]);
                break;
            }
            builder.append(array[i]).append(",");
        }
        return builder.toString();
    }

    private int[] stringToArray(String string){
        String[] line = string.split(",");
        int[] array = new int[line.length];
        for (int i = 0; i < line.length; i++){
            array[i] = Integer.parseInt(line[i]);
        }
        return array;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(flow);
        dataOutput.writeDouble(speed);
        dataOutput.writeUTF(arrayToString(positions));
        dataOutput.writeUTF(arrayToString(times));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.flow = dataInput.readDouble();
        this.speed = dataInput.readDouble();
        this.positions = stringToArray(dataInput.readUTF());
        this.times = stringToArray(dataInput.readUTF());
    }

    @Override
    public String toString() {
        return flow + "\t" + speed + "\t" + arrayToString(positions) + "\t" + arrayToString(times);
    }

    public double getFlow() {
        return flow;
    }

    public void setFlow(double flow) {
        this.flow = flow;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int[] getTimes() {
        return times;
    }

    public void setTimes(int[] times) {
        this.times = times;
    }

    public int[] getPositions() {
        return positions;
    }

    public void setPositions(int[] positions) {
        this.positions = positions;
    }
}
