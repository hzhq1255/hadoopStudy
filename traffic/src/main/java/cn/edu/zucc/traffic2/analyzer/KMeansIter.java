package cn.edu.zucc.traffic2.analyzer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/11 22:23
 * @desc:
 */
public class KMeansIter {

    public static class IterMapper extends Mapper<Object,Text,Text,Text>{
        int count = 0;
        int[] kind = new int[20000];
        double[][] k = {
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        };
        Text outLabel = new Text();
        Text outValue = new Text();

//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            URI localCacheFile = context.getCacheFiles()[0];
//            BufferedReader reader = new BufferedReader(new FileReader(localCacheFile.getPath()));
//            String line;
//            while ((line = reader.readLine()) != null) {
//                String[] currentLine = line.split("\t")[0].split(",");
//                for (int i = 0; i < 24; i ++){
//                    k[count][i] = Double.parseDouble(currentLine[i]);
//                }
//                count++;
//            }
//            count = 0;
//            reader.close();
//        }
//
//        @Override
//        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String[] line = value.toString().split("\t");
//            double[] vector = new double[24];
//            for (int i = 0; i < 24; i++){
//                vector[i] = Double.parseDouble(line[i+1]);
//            }
//            double distance = 99999999.0;
//            for (int i = 0; i < 3; i++) {
//                double currentDistance = StringUtil.getDistance(k[i], vector);
//                if (currentDistance < distance) {
//                    distance = currentDistance;
//                    kind[count] = i;
//                }
//            }
//            outLabel.set(String.valueOf(kind[count]));
//            count++;
//            outValue.set(StringUtil.vectorToString(vector));
//            context.write(outLabel, outValue);
//        }
    }
}
