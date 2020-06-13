package cn.edu.zucc.traffic1.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/10 16:50
 * @desc:
 */
public class SpeedReducer2 extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        double sum = 0;
        for (DoubleWritable val:values){
            count += 1;
            sum += val.get();
        }
        double avg = sum/count;
        avg = Double.parseDouble(String.format("%.2f",avg));
        context.write(new Text(key),new DoubleWritable(avg));
    }
}
