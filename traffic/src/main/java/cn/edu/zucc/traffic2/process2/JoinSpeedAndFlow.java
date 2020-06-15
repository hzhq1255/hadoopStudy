package cn.edu.zucc.traffic2.process2;

import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
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
 * @date: 2020/6/13 17:28
 * @desc:
 */
public class JoinSpeedAndFlow {

    private static class MyMapper extends Mapper<Object, Text, IntWritable, Text>{

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();
        String fileFlag = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            if (filename.contains("speed")){
                fileFlag = "l";
            }
            if (filename.contains("flow")){
                fileFlag = "r";
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line[] = value.toString().split("\t");
            outKey.set(Integer.parseInt(line[0]));
            outValue.set(line[1]+"\t"+fileFlag);
            context.write(outKey,outValue);
        }
    }

    private static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

        Text outValue = new Text();
        int count = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String flow = "";
            String speed = "";
            for (Text value:values){
                String[] line = value.toString().split("\t");
                if (line[1].equals("l")){
                    speed = line[0];
                }
                if (line[1].equals("r")){
                    flow = line[0];
                }
                count++;
            }
            StringBuilder builder = new StringBuilder();
            builder.append(speed).append("\t").append(flow);
            outValue.set(builder.toString());
            if (count > 1){
                context.write(key,outValue);
            }
            count = 0;
        }
    }

    public static void main(String[] args) throws Exception {

        String inPath1 = "E:/program/Hadoop/traffic/input2/flow/";
        String inPath2 = "E:/program/Hadoop/traffic/input2/speed/";
        String outPath = "E:/program/Hadoop/traffic/output2/speed_flow";
        int j = 1;
        for (int i = 1; i < 3; i ++){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"joinSpeedAndFlow");
            MyFileUtil.deleteDir(outPath+j);
            job.setJarByClass(JoinSpeedAndFlow.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job,new Path(inPath1+"flow"+i+".txt"));
            FileInputFormat.addInputPath(job,new Path(inPath2+"speed"+i+".txt"));
            FileOutputFormat.setOutputPath(job,new Path(outPath+j));
            System.out.println(job.waitForCompletion(true));
            j++;
        }


    }
}
