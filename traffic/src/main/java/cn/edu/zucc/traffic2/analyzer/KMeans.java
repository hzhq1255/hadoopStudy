package cn.edu.zucc.traffic2.analyzer;

import cn.edu.zucc.util.MyFileUtil;
import cn.edu.zucc.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/11 19:51
 * @desc:
 */
public class KMeans {



    public static class InitMapper extends Mapper<Object, Text, Text, Text> {
        double[][] k = {
                {588, 501, 370, 377, 222, 741, 991, 1283, 1364, 1383, 1414, 1683, 1518, 1538, 1572, 1690, 1800, 1814, 1702, 1212, 1525, 1317, 963, 716},
                {0, 1, 2, 2, 0, 14, 16, 16, 113, 42, 12, 8, 9, 2, 7, 9, 6, 0, 13, 1, 1, 0, 2, 1},
                {583, 404, 316, 273, 331, 561, 1063, 1399, 968, 92, 103, 85, 71, 87, 112, 117, 137, 128, 397, 1854, 1470, 1140, 1035, 714}
        };
        Text outLabel = new Text();
        Text outValue = new Text();
        int[] kind = new int[20000];
        int count = 0;
        int sum = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String centers = context.getConfiguration().get("center");
            if (!centers.equals("1")){
                String[] line = centers.split("\n");
                for (int i = 0; i < 3;i++){
                    String[] center = line[count].split(",");
                    for (int j = 0; j < 24; j++){
                        k[count][j] = Double.parseDouble(center[j]);
                    }
                    count++;
                }
                count = 0;
            }else {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(context.getConfiguration().get("centerFile"))));
                String line = "";
                while ((line = reader.readLine()) != null){
                    String[] center = line.split("\t")[0].split(",");
                    for (int i = 0; i < 24; i++){
                        k[count][i] = Double.parseDouble(center[i]);
                    }
                    count++;
                }
                count=0;
                reader.close();
            }

        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            sum+=1;
            String[] line = value.toString().split("\t");
            double[] vector = new double[24];
            for (int i = 0; i < 24; i++){
                vector[i] = Double.parseDouble(line[i+1]);
            }
            double distance = 99999999.0;
            for (int i = 0; i < 3; i++) {
                double currentDistance = StringUtil.getDistance(k[i], vector);
                if (currentDistance < distance) {
                    distance = currentDistance;
                    kind[count] = i;
                }
            }
            outLabel.set(String.valueOf(kind[count]));
            count++;
            outValue.set(StringUtil.vectorToString(vector)+","+line[0]);
            context.write(outLabel, outValue);
        }
    }

    public static class InitReducer extends Reducer<Text,Text,Text,Text>{

        double[][] k = {
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        };
        Text outKey = new Text();
        Text outValue = new Text();
        int kind = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] disVector = new double[24];
            double[] centerVector = new double[24];
            StringBuilder kindInclude = new StringBuilder();
            int count = 0;
            for (Text value:values){
                String[] line = value.toString().split(",");
                for (int i = 0; i < 24; i++){
                    disVector[i] += Double.parseDouble(line[i]);
                }

                if (values.iterator().hasNext()){
                    kindInclude.append(value.toString()).append("#");
                }
                else {
                    kindInclude.append(value.toString());
                }
                count++;
            }

            for (int i = 0; i < 24; i++){
                centerVector[i] = disVector[i]/count;
            }
            System.out.println(StringUtil.vectorToString(disVector));
            System.out.println(StringUtil.vectorToString(centerVector));
            outKey.set(StringUtil.vectorToString(centerVector));
            outValue.set(kindInclude.toString());
            System.arraycopy(centerVector, 0, k[kind], 0, 24);
            kind+=1;
            context.write(outKey,outValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(context.getConfiguration().get("centerFile"))));
            for (int i = 0; i < kind;i++){
                if (i == kind - 1){
                    writer.write(StringUtil.vectorToString(k[i]));
                }
                else {
                    writer.write(StringUtil.vectorToString(k[i])+"\n");
                }
                //System.out.println(StringUtil.vectorToString(k[i]));
            }
            writer.flush();
            writer.close();
        }
    }

    public static class SortMapper extends Mapper<Object,Text,Text,Text>{

        int count = 0;
        Text outKey = new Text();
        Text outValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] vectors = line[1].split("#");
            int vecLen = vectors.length;
            outValue.set(String.valueOf(count));
            count++;
            for (int i = 0; i < vecLen; i++){
                outKey.set(vectors[i]);
                context.write(outKey,outValue);
            }
        }
    }

    public static class SortReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text outKey = new Text();
            Text outValue = new Text();
            outValue.set(key);
            for (Text value:values){
                outKey.set(value);
                context.write(outKey,outValue);
            }
        }
    }





    public static void Sort(String[] paths,Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf,"KMeansSortJob");
        MyFileUtil.deleteDir(paths[1]);
        job.setJarByClass(KMeans.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(KPartitioner.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true));
    }


    public static void Init(String[] paths,Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf,"KMeansInitJob");
        startJob(paths, job);
    }




    public static void Iter(String[] paths,Configuration conf,int times) throws IOException, ClassNotFoundException, InterruptedException {
        for (int i = 0;i < times; i++){
            Job job = Job.getInstance(conf,"KMeansIterJob");
            startJob(paths, job);
        }
    }

    private static void startJob(String[] paths, Job job) throws IOException, InterruptedException, ClassNotFoundException {
//        if (job.getConfiguration().get("center").equals("") && paths[2] != null){
//            MyFileUtil.copyFile(paths[1]+"/part-r-00000",paths[2]);
//        }
        MyFileUtil.deleteDir(paths[1]);
        job.setJarByClass(KMeans.class);
        job.setMapperClass(InitMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(InitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(paths[0]));
        FileOutputFormat.setOutputPath(job,new Path(paths[1]));
        System.out.println(job.waitForCompletion(true));

    }
    /**
     * Job Driver
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String[] paths = new String[]{
                "E:/program/Hadoop/traffic/output/car_flow_combine/part-r-00000",
//                "E:/program/Hadoop/traffic/input1/test/test.txt",
                "E:/program/Hadoop/traffic/output2/kmeans/step",
                "E:/program/Hadoop/traffic/output2/kmeans/tmp/center.txt",
        };
        //496	394	367	319	316	546	1030	1167	909	62	73	90	92	125	137	124	109	108	1702	1837	1429	1176	1010	656
        String center =
                "675,591,546,564,572,981,1223,1500,1802,2130,2176,2259,2117,2369,2422,2607,2025,1385,1409,1317,1375,1253,1109,817\n"+
                        "0,1,2,2,0,14,16,16,113,42,12,8,9,2,7,9,6,0,13,1,1,0,2,1\n"+
                        "496,394,367,319,316,546,1030,1167,909,62,73,90,92,125,137,124,109,108,1702,1837,1429,1176,1010,656";
        //String center = StringUtil.getRandCenterString();
        Configuration configuration = new Configuration();
        configuration.set("center",center);
        configuration.set("centerFile","E:/program/Hadoop/traffic/output2/kmeans/tmp/center.txt");
        Init(paths,configuration);
        configuration.set("center","1");
        Iter(paths,configuration,10);
        String[] sortPaths = new String[]{
                "E:/program/Hadoop/traffic/output2/kmeans/step/part-r-00000",
                "E:/program/Hadoop/traffic/output2/result",
        };
        Sort(sortPaths,configuration);
    }
}
