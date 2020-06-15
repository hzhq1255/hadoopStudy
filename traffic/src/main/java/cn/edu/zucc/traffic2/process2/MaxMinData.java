package cn.edu.zucc.traffic2.process2;

import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/14 11:39
 * @desc:
 */
public class MaxMinData {

    private static class MaxMinMapper extends Mapper<Object, Text, Text, Text>{

        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            outKey.set("0");
            String[] line = value.toString().split("\t");
            String[] speedStrList = line[1].split(",");
            String[] flowStrList = line[2].split(",");
            for (String speedStr:speedStrList){
                double speed = Double.parseDouble(speedStr);
                outValue.set(speedStr);
                context.write(outKey,outValue);
            }
            for (String flowStr:flowStrList){
                double flow = Double.parseDouble(flowStr);
                outValue.set(flowStr);
                context.write(outKey,outValue);
            }
        }
    }

    private static class MaxMinReducer extends Reducer<Text,Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        double max = 0;
        double min = 999999999.0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:values){
                double num = Double.parseDouble(value.toString());
                if (max <= num){
                    max = num;
                }
                if (min >= num){
                    min = num;
                }
            }
            outKey.set(String.valueOf(min));
            outValue.set(String.valueOf(max));
            context.write(outKey,outValue);
        }
    }

    public static void main(String[] args) throws Exception{
        String inPath = "E:\\program\\Hadoop\\traffic\\output2\\speedAndFlow\\part-r-00000";
        String outPath = "E:\\program\\Hadoop\\traffic\\output2\\minAndMax";

        MyFileUtil.deleteDir(outPath);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"minAndMax");
        job.setJarByClass(MaxMinData.class);
        job.setMapperClass(MaxMinMapper.class);
        job.setReducerClass(MaxMinReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.out.println(job.waitForCompletion(true));
    }
}
