package cn.edu.zucc.traffic1.wayspeed;

import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/15 15:18
 * @desc: 车道平均速度 第一步
 */
public class WaySpeedTask1 {

    private static class MyMapper1 extends Mapper<Object, Text, Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        int index = 0;
        int count = 0;
        int fileFlag = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            count = 0;
            index = 0;
            String num = filename.split("\\.")[0];
            fileFlag = Integer.parseInt(num);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (count == 0){
                index++;
            }
            count = (count+1) % 2;
            outKey.set(fileFlag+"\t"+index);
            System.out.println(fileFlag+"\t"+index+"\t"+count);
            // 两天给的格式不一样
            //String[] line = value.toString().split(" ");
            String[] line = value.toString().split("\t");
            //0008f05817e62e07ef521d38172a491e 02 K33 079100 2 2019-08-01 08:04:19
            String kmStr = line[3].substring(0,3);
            String mStr = line[3].substring(3,6);
            double km = kmStr.charAt(0) == 0  ? Double.parseDouble(kmStr.substring(1,2)):Double.parseDouble(kmStr.substring(0,3));
            double m = mStr.charAt(0) == 0  ? Double.parseDouble(mStr.substring(1,2)):Double.parseDouble(mStr.substring(0,3));
            double dist = km+m/1000;
            //long time = Timestamp.valueOf(line[5]+" "+line[6]).getTime();
            long time = Timestamp.valueOf(line[5]).getTime();
            outValue.set(line[4]+"\t"+dist+"\t"+time);
            context.write(outKey,outValue);
        }
    }

    private static class MyReducer1 extends Reducer<Text, Text, Text, Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        double[] dist = new double[2];
        long[] time = new long[2];
        int[] way = new int[2];
        double speed = 0;
        int wayCode = 0;
        int count = 0;


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Arrays.fill(dist,0);
            Arrays.fill(time,0);
            Arrays.fill(way,0);
            //System.out.println(key.toString());
            for (Text value:values){
                String[] line = value.toString().split("\t");
                way[count] = Integer.parseInt(line[0]);
                dist[count] = Double.parseDouble(line[1]);
                time[count] = Long.parseLong(line[2]);
                count++;
            }
            double passDist = dist[0] - dist[1] > 0 ? dist[0] - dist[1] : dist[1] - dist[0];
            long spendTime = time[0] - time[1] > 0 ? time[0] - time[1] : time[1] - time[0];
            speed = passDist*(60*60*1000)/spendTime;
            if (way[0] == way[1]){
                if (speed >= 60.0 && speed <= 120.0){
                    wayCode = way[0];
                    outKey.set(String.valueOf(wayCode));
                    outValue.set(String.valueOf(speed));
                    context.write(outKey,outValue);
                }
            }
            speed = 0;
            count = 0;
        }
    }

    public static void main(String[] args) throws Exception {
        String inPath = "E:\\program\\Hadoop\\traffic\\input\\speed1\\";
        String outPath = "E:\\program\\Hadoop\\traffic\\output5\\wayspeed2";

        MyFileUtil.deleteDir(outPath);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"waySpeedTask1");
        job.setJarByClass(WaySpeedTask1.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < 19; i++){
            FileInputFormat.addInputPath(job,new Path(inPath+i+".txt"));
        }
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.out.println(job.waitForCompletion(true));

    }
}
