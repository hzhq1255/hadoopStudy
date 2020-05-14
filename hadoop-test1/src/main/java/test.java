import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.util.Date;

public class test {
    public static void main(String[] args){
        try {
            String fileName = "hdfs://192.168.71.128:9000/input/Hello.txt";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            String currentTime = (new Date()).toString();
            if (fs.exists(new Path(fileName))){
                System.out.println("文件存在"+" "+ currentTime);
            }else {
                System.out.println("文件不存在"+" "+ currentTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
