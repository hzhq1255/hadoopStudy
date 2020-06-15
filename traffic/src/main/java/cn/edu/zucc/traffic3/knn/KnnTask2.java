package cn.edu.zucc.traffic3.knn;

import cn.edu.zucc.util.MyFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/14 10:23
 * @desc:
 */
public class KnnTask2 {


    public static class KnnMapper2 extends Mapper<Object,Text, DistBean, Text>{

        DistBean outKey = new DistBean();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            double distance = Double.parseDouble(line[0]);
            String testStr = line[1]+"\t"+line[2]+"\t"+line[3]+"\t"+line[4];
            int index1 = Integer.parseInt(line[5]);
            String trainStr = line[6]+"\t"+line[7]+"\t"+line[8]+"\t"+line[9];
            int index2 = Integer.parseInt(line[10]);
            DataBean testData = new DataBean(testStr);
            DataBean trainData = new DataBean(trainStr);
            outKey.setData(testData);
            outKey.setDistance(distance);
            outKey.setIndex(index1);
            outValue.set(trainData.toString()+"\t"+index2);
            context.write(outKey,outValue);
        }
    }

    public static class KnnReducer2 extends Reducer<DistBean,Text,Text,Text>{

        Text outKey = new Text();
        Text outValue = new Text();
        int index = -1;
        int neighbors = 7;
        int count = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            neighbors = Integer.parseInt(context.getConfiguration().get("k"));
        }

        @Override
        protected void reduce(DistBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value:values){
                String[] line = value.toString().split("\t");
                int dataIndex = key.getIndex();
                outKey.set(key.toString());
                outValue.set(line[0]+"\t"+line[1]+"\t"+line[2]+"\t"+line[3]+"\t"+line[4]);
                if (index != dataIndex){
                    index = dataIndex;

                    //写入
                    context.write(outKey,outValue);
                    if (count != 0){
                        count = 0;
                    }
                    count++;
                }else {
                    if (count < neighbors){
                        //写入
                        context.write(outKey,outValue);
                        count++;
                    }
                }
                //System.out.println("index "+index+" count "+count);
            }
//            for (Text value:values){
//                String[] line = value.toString().split("\t");
//                outKey.set(key.toString());
//                outValue.set(line[0]+"\t"+line[1]+"\t"+line[2]+"\t"+line[3]+"\t"+line[4]);
//                context.write(outKey,outValue);
//            }
        }
    }



    public static void main(String[] args,int k) throws Exception {

        String inPath = "E:\\program\\Hadoop\\traffic\\output3\\distance\\part-r-00000";
        String outPath = "E:\\program\\Hadoop\\traffic\\output3\\sortDistance";

        MyFileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();
        conf.set("k",String.valueOf(k));
        Job job = Job.getInstance(conf,"sortDistance");
        job.setJarByClass(KnnTask2.class);
        job.setMapperClass(KnnMapper2.class);
        job.setReducerClass(KnnReducer2.class);
        job.setMapOutputKeyClass(DistBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.out.println(job.waitForCompletion(true));
    }
}
