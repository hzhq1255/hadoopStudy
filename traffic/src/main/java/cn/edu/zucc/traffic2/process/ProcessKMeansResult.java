package cn.edu.zucc.traffic2.process;


import cn.edu.zucc.util.HashDirection;
import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/12 19:13
 * @desc:
 */
public class ProcessKMeansResult {

    public static class ResultMapper extends Mapper<Object,Text, Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] values = line[1].split(",");
            String[] keys = values[24].split("_");
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 24; i++){
                if (i == 23){
                    builder.append(values[i]);
                    continue;
                }
                builder.append(values[i]).append(",");
            }
            outKey.set(keys[0]);
            outValue.set(builder.toString()+"\t"+keys[1]);
            context.write(outKey,outValue);
        }
    }

    public static class ResultReducer extends Reducer<Text,Text,IntWritable,Text> {

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();
        List<String> textList = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            outKey.set(HashDirection.capPosition.get(k));
            for (Text val:values){
                //System.out.println(val.toString());
                textList.add(val.toString());
            }
            if (textList.size() == 2){
                for (String val:textList){
                    StringBuilder builder = new StringBuilder(val);
                    String str = builder.toString();
                    outValue.set(str);
                    System.out.println(val);
                    context.write(outKey,outValue);
                }
            }
            textList.clear();
        }
    }

    public static class ResultMapper1 extends Mapper<Object,Text,IntWritable,Text>{

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            outKey.set(Integer.parseInt(line[2]));
            outValue.set(line[0]+"\t"+line[1]);
            context.write(outKey,outValue);
        }
    }

    public static class ResultReducer1 extends Reducer<IntWritable,Text,IntWritable,Text>{

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val:values){
                String[] line = val.toString().split("\t");
                outKey.set(Integer.parseInt(line[0]));
                outValue.set(line[1]);
                context.write(outKey,outValue);
            }

        }
    }

    public static class SortMapper extends Mapper<Object,Text,IntWritable,Text>{

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            outKey.set(Integer.parseInt(line[0]));
            outValue.set(line[1]);
            context.write(outKey,outValue);
        }
    }

    public static class ResultPartitioner extends Partitioner<IntWritable,Text>{

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            int k = key.get();
            switch (k){
                case 1:return 0;
                case 2:return 1;
                default:return 2;
            }
        }

    }

    public static void resultDriver() throws Exception{
        String[] paths = new String[]{
                "E:/program/Hadoop/traffic/output2/result/part-r-00000",
                "E:/program/Hadoop/traffic/output2/result/part-r-00001",
                "E:/program/Hadoop/traffic/output2/result1",
        };

        Job job = Job.getInstance(conf,"resultDriver");
        MyFileUtil.deleteDir(paths[2]);
        job.setJarByClass(ProcessKMeansResult.class);
        job.setMapperClass(ResultMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        job.setPartitionerClass(ResultPartitioner.class);
//        job.setNumReduceTasks(2);
        job.setReducerClass(ResultReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileInputFormat.addInputPath(job,new Path(paths[1]));
        FileOutputFormat.setOutputPath(job,new Path(paths[2]));
        System.out.println(job.waitForCompletion(true));
    }


    public static void resultDriver1() throws Exception{
        String[] paths = new String[]{
                "E:/program/Hadoop/traffic/output2/result1/part-r-00000",
                "E:/program/Hadoop/traffic/output2/result2",
        };
        MyFileUtil.deleteDir(paths[1]);
        Job job1 = Job.getInstance(conf,"resultDriver1");
        job1.setJarByClass(ProcessKMeansResult.class);
        job1.setMapperClass(ResultMapper1.class);
        job1.setReducerClass(ResultReducer1.class);
        job1.setPartitionerClass(ResultPartitioner.class);
        job1.setNumReduceTasks(2);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job1,new Path(paths[1]));
        System.out.println(job1.waitForCompletion(true));
    }

    public static void sortDriver() throws Exception {
        String[] paths = new String[]{
                "E:/program/Hadoop/traffic/output2/result2/part-r-00000",
                "E:/program/Hadoop/traffic/output2/train",
                "E:/program/Hadoop/traffic/output2/result2/part-r-00001",
                "E:/program/Hadoop/traffic/output2/test"
        };
        MyFileUtil.deleteDir(paths[1]);
        MyFileUtil.deleteDir(paths[3]);
        for (int i = 0; i < 2; i++){
            Job job = Job.getInstance(conf,"resultDriver1");
            job.setJarByClass(ProcessKMeansResult.class);
            job.setMapperClass(SortMapper.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(IntWritable.class);
            FileInputFormat.addInputPath(job,new Path(paths[2*i]));
            FileOutputFormat.setOutputPath(job,new Path(paths[2*i+1]));
            System.out.println(job.waitForCompletion(true));
        }


    }


    private static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception {

        //resultDriver();
        //resultDriver1();
        sortDriver();
    }
}
