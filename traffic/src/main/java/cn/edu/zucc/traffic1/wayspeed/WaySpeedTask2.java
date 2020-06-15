package cn.edu.zucc.traffic1.wayspeed;

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
 * @date: 2020/6/15 17:01
 * @desc:
 */
public class WaySpeedTask2 {

    protected static class MyMapper2 extends Mapper<Object, Text, IntWritable, Text>{

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

    protected static class MyReducer2 extends Reducer<IntWritable, Text, Text, Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        double sum = 0;
        int count = 0;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value:values){
                sum += Double.parseDouble(value.toString());
                count++;
            }
            double avgSpeed = sum/count;
            sum = 0;
            count = 0;
            outKey.set(key.toString());
            outValue.set(String.valueOf(avgSpeed));
            context.write(outKey,outValue);
        }
    }

    public static void main(String[] args) throws Exception {

        String[] inPaths = new String[]{
                "E:\\program\\Hadoop\\traffic\\output5\\wayspeed1\\part-r-00000",
                "E:\\program\\Hadoop\\traffic\\output5\\wayspeed2\\part-r-00000"
        };
        String outPath = "E:\\program\\Hadoop\\traffic\\output5\\avgWaySpeed";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"avgWaySpeedTask");
        job.setJarByClass(WaySpeedTask2.class);
        job.setMapperClass(MyMapper2.class);
        job.setReducerClass(MyReducer2.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(inPaths[0]));
        FileInputFormat.addInputPath(job,new Path(inPaths[1]));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.out.println(job.waitForCompletion(true));
    }
}
