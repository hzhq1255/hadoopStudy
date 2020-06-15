package cn.edu.zucc.traffic2.process2;

import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/13 19:21
 * @desc:
 */
public class OneHot {


    private static String positionToString(int[] position){
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i < position.length -1; i++){
            builder.append(position[i]).append(",");
        }
        builder.append(position[position.length-1]);

        return builder.toString();
    }

    private static String timeToString(int[] time){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < time.length -1; i++){
            builder.append(time[i]).append(",");
        }
        builder.append(time[time.length-1]);
        return builder.toString();
    }

    private static int[] initArray(int[] array){

        Arrays.fill(array, 0);
        return array;
    }

    private static class MyMapper extends Mapper<Object, Text, Text, Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        double max = 4354.0;
        double min = 0.0;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] speedStr = line[1].split(",");
            String[] flowStr = line[2].split(",");
            for (int i = 0; i < 24; i++){
                double speed = (Double.parseDouble(speedStr[i])-min)/(max-min);
                double flow = (Double.parseDouble(flowStr[i])-min)/(max-min);
                outKey.set(line[0]+"\t"+speed +"\t"+i);
                outValue.set(String.valueOf(flow));
                if (Double.parseDouble(speedStr[i]) > 0.0){
                    context.write(outKey,outValue);
                }
            }
        }
    }

    private static class MyReducer extends Reducer<Text,Text,DoubleWritable,Text>{

        DoubleWritable outKey = new DoubleWritable();
        Text outValue = new Text();
        double max = 4354.0;
        double min = 0.0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] position = new int[21];
            int[] time = new int[24];
            OneHot.initArray(position);
            OneHot.initArray(time);
            String[] keys = key.toString().split("\t");
            int index1 = Integer.parseInt(keys[0]);
            int index2 = Integer.parseInt(keys[2]);
            position[index1] = 1;
            time[index2] = 1;
            outValue.set(keys[1]+"\t"+OneHot.positionToString(position)+"\t"+OneHot.timeToString(time));
            for (Text value:values){
                outKey.set(Double.parseDouble(value.toString()));
                context.write(outKey,outValue);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        String[] paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\input4\\train\\speedflow2.txt",
                "E:\\program\\Hadoop\\traffic\\output6\\train",
        };
//        String[] paths = new String[]{
//                "E:\\program\\Hadoop\\traffic\\input4\\test\\speedflow1.txt",
//                "E:\\program\\Hadoop\\traffic\\output6\\test",
//        };
        MyFileUtil.deleteDir(paths[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"oneHot");
        job.setJarByClass(OneHot.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true));
    }
}
