package cn.edu.zucc.traffic1.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/10 16:50
 * @desc:
 */
public class SpeedReducer2 extends Reducer<IntWritable, Text,IntWritable,Text> {

    IntWritable outKey = new IntWritable();
    Text outValue = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        double sum = 0;
        String position = "";
        for (Text val:values){
            String[] line = val.toString().split("\t");
            position = line[1];
            double speed = Double.parseDouble(line[0]);
            count += 1;
            sum += speed;
        }
        double avg = sum/count;
        avg = Double.parseDouble(String.format("%.2f",avg));
        outValue.set(avg +"\t"+position);
        context.write(key,outValue);
    }
}
