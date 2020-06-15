package cn.edu.zucc.traffic2.process2;

import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * @date: 2020/6/14 10:43
 * @desc:
 */
public class AvgSpeed {

    private static class AvgSpeedMapper extends Mapper<Object, Text, Text, Text>{

        Text outKey = new Text();
        Text outValue =  new Text();


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            outKey.set("1");
            String[] line = value.toString().split("\t");
            String[] speedStrList = line[1].split(",");
            for (String speedStr:speedStrList){
                double speed = Double.parseDouble(speedStr);
                if (speed > 0.0){
                    outValue.set(speedStr);
                    context.write(outKey,outValue);
                }
            }
        }
    }

    private static class AvgSpeedReducer extends Reducer<Text,Text,Text,Text> {

        Text outKey = new Text();
        Text outValue =  new Text();
        int count = 0;
        double sum = 0;
        double avg = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value:values){
                sum+=Double.parseDouble(value.toString());
                count++;
            }
            avg = sum/count;
            outKey.set(String.valueOf(count));
            outValue.set(String.valueOf(avg));
            context.write(outKey,outValue);
            count = 0;
            sum = 0;
            avg = 0;
        }
    }

    public static void main(String[] args) throws Exception {

        args = new String[]{
                "E:\\program\\Hadoop\\traffic\\output2\\speedAndFlow\\part-r-00000",
                "E:\\program\\Hadoop\\traffic\\output2\\avgSpeed"
        };
        MyFileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"avgSpeedName");
        job.setJarByClass(AvgSpeed.class);
        job.setMapperClass(AvgSpeedMapper.class);
        job.setReducerClass(AvgSpeedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.out.println(job.waitForCompletion(true));
    }

}
