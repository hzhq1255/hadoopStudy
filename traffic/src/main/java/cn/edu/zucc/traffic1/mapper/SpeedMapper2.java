package cn.edu.zucc.traffic1.mapper;

import cn.edu.zucc.util.HashDirection;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/10 16:43
 * @desc:
 */
public class SpeedMapper2 extends Mapper<Object, Text, IntWritable, Text> {

    IntWritable k = new IntWritable();
    Text v = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        int p1 = HashDirection.capPosition.get(line[3]);
        int p2 = HashDirection.capPosition.get(line[4]);
        String position = "";
        String hour = line[1].charAt(0) == '0'? line[1].substring(1,2):line[1];
        if (p1>p2){
            position = line[3];
        }else {
            position = line[4];
        }
        k.set(Integer.parseInt(hour));
        v.set(line[2]+"\t"+position);
        context.write(k,v);
    }
}
