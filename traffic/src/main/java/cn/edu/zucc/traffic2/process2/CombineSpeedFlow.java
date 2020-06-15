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
 * @date: 2020/6/13 19:28
 * @desc:
 */
public class CombineSpeedFlow {

    private static class MyMapper extends Mapper<Object, Text, IntWritable, Text>{

        IntWritable outKey = new IntWritable();
        Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            if (filename.contains("1")){
                outKey.set(1);
            }
            if (filename.contains("2")){
                outKey.set(2);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            outValue.set(line[0]+"\t"+line[1]+"\t"+line[2]);
            context.write(outKey,outValue);
        }

    }

    private static class MyReducer extends Reducer<IntWritable,Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                String[] line = val.toString().split("\t");
                outKey.set(line[0]);
                outValue.set(line[1]+"\t"+line[2]);
                context.write(outKey,outValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String[] paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\input2\\speedAndFlow",
                "E:\\program\\Hadoop\\traffic\\output2\\speedAndFlow",
        };

        MyFileUtil.deleteDir(paths[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"combineSpeedAndFlow");
        job.setJarByClass(CombineSpeedFlow.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true));
    }
}
