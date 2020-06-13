package cn.edu.zucc.traffic1.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/10 16:43
 * @desc:
 */
public class SpeedMapper2 extends Mapper<Object, Text,Text, DoubleWritable> {

    Text k = new Text();
    DoubleWritable v = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        k.set(line[1]);
        v.set(Double.parseDouble(line[2]));
        context.write(k,v);
    }
}
