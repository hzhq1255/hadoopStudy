package cn.edu.zucc.traffic1;

import cn.edu.zucc.traffic1.job.SpeedJob1;
import cn.edu.zucc.traffic1.job.SpeedJob2;

import java.io.IOException;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/10 17:10
 * @desc:
 */
public class Test {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for (int i = 0 ; i < 19 ; i++){
            String[] paths =  new String[]{
                    "E:\\program\\Hadoop\\traffic\\output\\speed\\"+i+"\\part-r-00000",
                    "E:\\program\\Hadoop\\traffic\\output1\\speed\\"+i
            };
            //SpeedJob1.main(paths);
            SpeedJob2.main(paths);
        }
        System.exit(0);
    }
}
