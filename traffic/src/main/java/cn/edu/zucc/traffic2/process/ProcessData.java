package cn.edu.zucc.traffic2.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/11 15:37
 * @desc:
 */
public class ProcessData {

    public static class ProcessMapper1 extends Mapper<Object, Text,Text, IntWritable>{
        Text outKey = new Text();
        IntWritable outValue = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            outKey.set(line[1]);
            outValue.set(Integer.parseInt(line[0]));
            context.write(outKey,outValue);
        }
    }

    /** 统计每个卡点总的车流量
     *
     */
    public static class ProcessReducer1 extends Reducer<Text,IntWritable,IntWritable,Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            for (IntWritable val: values){
                sum+=val.get();
            }
            context.write(new IntWritable(sum),key);
        }
    }

    public static void sum(String[] paths) throws IOException, ClassNotFoundException, InterruptedException {
        paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\input\\car_flow\\car_flow2.txt",
                "E:\\program\\Hadoop\\traffic\\output\\car_flow2_sum"
        };
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"processDataSum");
        job.setJarByClass(ProcessData.class);
        job.setMapperClass(ProcessMapper1.class);
        job.setReducerClass(ProcessReducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true)+"\n");
    }

    /**
     * 补充24小时的数据
     */
    public static class ProcessMapper2 extends Mapper<Object,Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            outKey.set(line[1]);
            outValue.set(line[2]);
            context.write(outKey,outValue);
        }
    }

    public static class ProcessReducer2 extends Reducer<Text,Text,IntWritable,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val:values){
                sum+=1;
            }
            context.write(new IntWritable(sum),key);
        }
    }

    public static void completeFlow(String[] paths) throws IOException, ClassNotFoundException, InterruptedException{
        paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\input\\car_flow\\car_flow1.txt",
                "E:\\program\\Hadoop\\traffic\\output\\car_flow1_hour_sum"
        };
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"processDataSum");
        job.setJarByClass(ProcessData.class);
        job.setMapperClass(ProcessMapper2.class);
        job.setReducerClass(ProcessReducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true)+"\n");
    }

    public static class ProcessMapper3 extends Mapper<Object,Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        String date = "";
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            String fileName = path.getName();
            if (fileName.contains("car_flow1.txt")){
                date = "1";
            }
            else if (fileName.contains("car_flow2.txt")){
                date = "2";
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(" ");
            outKey.set(line[1]+"_"+date);
            outValue.set(line[2]+" "+line[0]);
            context.write(outKey,outValue);
        }
    }

    public static class ProcessReducer3 extends Reducer<Text,Text,Text,Text>{

        Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] hourFlow = new String[24];
            StringBuilder hourFlows = new StringBuilder();
            for (Text val:values){
                String[] line = val.toString().split(" ");
                Integer index = Integer.parseInt(line[0]);
                hourFlow[index] = line[1];
                //hourFlows.append(val).append("\t");
            }
            for (int i = 0;i < hourFlow.length ; i++){
                if (hourFlow[i] == null){
                    hourFlow[i] = "0";
                }
                if (i == hourFlow.length - 1){
                    hourFlows.append(hourFlow[i]);
                }else {
                    hourFlows.append(hourFlow[i]).append("\t");
                }
            }
            outValue.set(String.valueOf(hourFlows));
            context.write(key,outValue);
        }
    }

    public static void combineFlow(String[] paths) throws IOException, ClassNotFoundException, InterruptedException {
        paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\input\\car_flow\\",
                "E:\\program\\Hadoop\\traffic\\output\\car_flow_combine"
        };
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf,"processDataCombineFlow");
        job.setJarByClass(ProcessData.class);
        job.setMapperClass(ProcessMapper3.class);
        job.setReducerClass(ProcessReducer3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true)+"\n");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        System.exit(job.waitForCompletion(true)?0:1);

        //completeFlow(args);
        combineFlow(args);
    }
}
