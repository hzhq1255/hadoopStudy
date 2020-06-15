package cn.edu.zucc.traffic3.knn;

import cn.edu.zucc.util.MyFileUtil;
import cn.edu.zucc.util.StringUtil;
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
import java.util.ArrayList;
import java.util.List;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/12 22:16
 * @desc:
 */
public class KnnTask1 {

    // 最近邻的3个
    public static class KnnMapper1 extends Mapper<Object,Text, Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        String fileFlag = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (filename.contains("test")){
                fileFlag = "1";
            }
            if (filename.contains("train")){
                fileFlag = "2";
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            outKey.set("0");
            outValue.set(value.toString()+"\t"+fileFlag);
            context.write(outKey,outValue);
        }
    }

    public static class KnnReducer1 extends Reducer<Text, Text, DoubleWritable, Text>{

        DoubleWritable outKey = new DoubleWritable();
        Text outValue = new Text();
        List<DataBean> testDataList = new ArrayList<>();
        List<DataBean> trainDataList = new ArrayList<>();
        int count1 =0;
        int count2 =0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                String[] line = val.toString().split("\t");
                String str = line[0]+"\t"+line[1]+"\t"+line[2]+"\t"+line[3];
                if (line[4].equals("1")){
                    testDataList.add(new DataBean(str));
                    count1++;
                }
                if (line[4].equals("2")){
                    trainDataList.add(new DataBean(str));
                    count2++;
                }
            }
            int index1 = 0;
            int index2 = 0;
            for (DataBean testData:testDataList){

                for (DataBean trainData:trainDataList){
                    double distance = StringUtil.getKnnDistance(testData,trainData);
                    outKey.set(distance);
                    outValue.set(testData.toString()+"\t"+index1+"\t"+trainData.toString()+"\t"+index2);
                    context.write(outKey,outValue);
                    index2++;
                }
                index1++;
            }
            count1=0;
            count2=0;
        }
    }

    public static void main(String[] args) throws Exception{
//        args = new String[]{
//                "E:\\program\\Hadoop\\traffic\\output3\\test\\part-r-00000",
//                "E:\\program\\Hadoop\\traffic\\output3\\train\\part-r-00000",
//                "E:\\program\\Hadoop\\traffic\\output3\\distance"
//        };
        MyFileUtil.deleteDir(args[2]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"knnDistance");
        job.setJarByClass(KnnTask1.class);
        job.setMapperClass(KnnMapper1.class);
        job.setReducerClass(KnnReducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        System.out.println(job.waitForCompletion(true));
    }

}
