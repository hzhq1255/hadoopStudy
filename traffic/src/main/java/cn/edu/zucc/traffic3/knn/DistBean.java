package cn.edu.zucc.traffic3.knn;

import cn.edu.zucc.traffic1.bean.SpeedResult;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/14 10:24
 * @desc:
 */
public class DistBean implements WritableComparable<DistBean> {

    private DataBean data;
    private int index;
    private double distance;

    public DistBean() {
    }

    public DistBean(DataBean data, int index, double distance) {
        this.data = data;
        this.index = index;
        this.distance = distance;
    }

    @Override
    public String toString() {
        return distance+"\t"+data.toString()+"\t"+index;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(data.toString());
        dataOutput.writeInt(index);
        dataOutput.writeDouble(distance);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.data = new DataBean(dataInput.readUTF());
        this.index = dataInput.readInt();
        this.distance = dataInput.readDouble();
    }


    @Override
    public int compareTo(DistBean o) {
        int compareIndex = this.index - o.index;
        double compareDistance = this.distance - o.distance;;
        if (compareIndex == 0){
            if (compareDistance > 0){
                return 1;
            }else {
                return -1;
            }
        }
        return compareIndex;
    }

    public DataBean getData() {
        return data;
    }

    public void setData(DataBean data) {
        this.data = data;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
