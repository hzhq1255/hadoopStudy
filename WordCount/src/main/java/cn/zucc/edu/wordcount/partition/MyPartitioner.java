package cn.zucc.edu.wordcount.partition;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/5/14 14:09
 * @desc:
 *
 */
public class MyPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String firstString = key.toString().substring(0, 1);
        char firstChar = firstString.charAt(0);
        if (firstString.matches("[a-zA-Z]")) {
            if (firstString.matches("[a-z]")){
                return firstChar - 96;
            }
            else {
                return firstChar - 64;
            }

        } else {
            return 0;
        }
    }
}
