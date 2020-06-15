package cn.edu.zucc.traffic2.process2;

import cn.edu.zucc.util.HashDirection;
import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @date: 2020/6/13 15:06
 * @desc:
 */
public class CombineSpeed {

    private static class MyMapper extends Mapper<Object,Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            outKey.set(line[2]);
            outValue.set(line[0]+"\t"+line[1]);
            context.write(outKey,outValue);
        }

    }

    private static class MyReducer extends Reducer<Text,Text, Text,Text> {
        Text outKey = new Text();
        Text outValue = new Text();
        double[] speed = new double[24];

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 24; i++){
                speed[i] = 0.0;
            }
            for (Text val:values){
                String[] line = val.toString().split("\t");
                int index = Integer.parseInt(line[0]);
                speed[index] = Double.parseDouble(line[1]);
            }
            for (int i = 0; i < 24; i++){
                if (i == 23){
                    builder.append(speed[i]);
                }else {
                    builder.append(speed[i]).append(",");
                }
            }
            String k = HashDirection.capPosition.get(key.toString()).toString();
            System.out.println(k);
            outKey.set(k);
            outValue.set(builder.toString());
            context.write(outKey,outValue);

        }
    }

    public static void main(String[] args) throws Exception {
        String inPath = "E:/program/Hadoop/traffic/output4/speed/";
        String outPath = "E:/program/Hadoop/traffic/output5/speed/";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"speedCombine");
        MyFileUtil.deleteDir(outPath);
        job.setJarByClass(CombineSpeed.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < 19;i++){
            FileInputFormat.addInputPath(job,new Path(inPath+i+"/part-r-00000"));
        }
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.out.println(job.waitForCompletion(true));
    }
}
