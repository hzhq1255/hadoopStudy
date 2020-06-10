package cn.edu.zucc.traffic1.job;

import cn.edu.zucc.traffic1.bean.SpeedMap;
import cn.edu.zucc.traffic1.bean.SpeedResult;
import cn.edu.zucc.traffic1.mapper.SpeedMapper;
import cn.edu.zucc.traffic1.reduce.SpeedReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/10 0:15
 * @desc:
 */
public class SpeedJob1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{
                "E:\\program\\Hadoop\\traffic\\input\\speed\\",
                "E:\\program\\Hadoop\\traffic\\output\\speed\\"
        };
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"trafficSpeed");
        job.setJarByClass(SpeedJob1.class);
        job.setMapperClass(SpeedMapper.class);
        job.setReducerClass(SpeedReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeedMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeedResult.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}