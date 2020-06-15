package cn.edu.zucc.traffic2.analyzer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/12 16:16
 * @desc:
 */
public class KPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPart) {
        String v = value.toString();
        String[] line = key.toString().split(",");
        String k = line[line.length -1].substring(7,8);
        String e = v+k;
        return Integer.parseInt(v);
//        switch (e){
//            case "01":return 0;
//            case "02":return 1;
//            case "11":return 2;
//            case "12":return 3;
//            case "21":return 4;
//            case "22":return 5;
//            default:return 6;
//        }
    }

    public static void main(String[] args) {
        String str = "110410_2";
        System.out.println(str.substring(7,8));
    }
}
