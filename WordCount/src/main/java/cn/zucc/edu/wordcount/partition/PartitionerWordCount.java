package cn.zucc.edu.wordcount.partition;

import cn.zucc.edu.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/5/14 14:33
 * @desc:
 */
public class PartitionerWordCount {
    public static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }
        Text k = new Text();
        Text v = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line);
            while(st.hasMoreTokens()){
                k.set(st.nextToken());
                context.write(k,v);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }
        Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Text text : values){
                counter += Integer.parseInt(text.toString());
            }
            v.set(counter+"");
            context.write(key,v);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static void main(String[] args){
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"wordcount");
            job.setJarByClass(PartitionerWordCount.class);
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(27);
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //添加文件的输入路径
            FileInputFormat.addInputPath(job, new Path(args[0]));
            //添加文件的输出路径
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
