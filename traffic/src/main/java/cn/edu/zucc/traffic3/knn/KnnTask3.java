package cn.edu.zucc.traffic3.knn;

import cn.edu.zucc.util.HashDirection;
import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * @date: 2020/6/14 14:11
 * @desc:
 */
public class KnnTask3 {

    public static class KnnMapper3 extends Mapper<Object, Text, Text, DoubleWritable>{

        Text outKey = new Text();
        DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String testStr = line[1]+"\t"+line[2]+"\t"+line[3]+"\t"+line[4];
            double flow = Double.parseDouble(line[6]);
            outKey.set(testStr);
            outValue.set(flow);
            context.write(outKey,outValue);
        }
    }

    public static class KnnReducer3 extends Reducer<Text,DoubleWritable,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        double mse = 0;
        double pe = 0;
        int countData = 0;
        double max = 4354.0;
        double min = 0.0;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable value:values){
                sum += value.get();
                count++;
            }
            double predict = sum/count;
            String[] line = key.toString().split("\t");
            double flow = Double.parseDouble(line[0]);
            double error = Math.pow(predict-flow,2.0);
            double percent = Math.sqrt(Math.pow((flow-predict),2.0))/flow;
            pe += percent;
            System.out.println(flow+"\t"+predict+"\t"+percent);
            mse += error;
            //outValue.set(predict+"\t"+error);
            double flow1 = Double.parseDouble(line[0])*(max-min)+min;
            double speed1 = Double.parseDouble(line[1])*(max-min)+min;
            String[] times = line[3].split(",");
            int time = 0;
            for (int i = 0; i < times.length; i++){
                if ( Integer.parseInt(times[i]) == 1){
                    time = i;
                }
            }
            String[] positions = line[2].split(",");
            int position = 0;
            for (int i = 0; i < positions.length;i++){
                if (Integer.parseInt(positions[i]) == 1){
                    position = i;
                }
            }
            //System.out.println(position+"\t"+time);
            double predict1 = predict*(max-min)+min;
            flow1 = Double.parseDouble(String.format("%.2f",flow1));
            speed1 = Double.parseDouble(String.format("%.2f",speed1));
            predict1 = Double.parseDouble(String.format("%.2f",predict1));
            outValue.set(predict1+"\t"+error);
            outKey.set(flow1+"\t"+speed1+"\t"+ HashDirection.codePosition.get(position+1) +"\t"+time);
            //outKey.set(flow1+"\t"+speed1+"\t"+ line[2] +"\t"+line[3]);
            context.write(outKey,outValue);
            countData++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            pe = pe/countData;
            pe = Double.parseDouble(String.format("%.4f",pe));
            context.write(new Text(String.valueOf(mse)),new Text(pe+"%"));
        }
    }


    public static void main(String[] args) throws Exception {

        String inPath = "E:\\program\\Hadoop\\traffic\\output3\\sortDistance\\part-r-00000";
        String outPath = "E:\\program\\Hadoop\\traffic\\output3\\predict7";


        MyFileUtil.deleteDir(args[1]);
        Configuration conf =  new Configuration();
        Job job = Job.getInstance(conf,"predictResult");
        job.setJarByClass(KnnTask3.class);
        job.setMapperClass(KnnMapper3.class);
        job.setReducerClass(KnnReducer3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.out.println(job.waitForCompletion(true));
    }
}
