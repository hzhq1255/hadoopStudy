package cn.edu.zucc.traffic1.mapper;

import cn.edu.zucc.traffic1.bean.SpeedBean;
import lombok.SneakyThrows;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/9 21:27
 * @desc:
 */
public class SpeedMapper extends Mapper<Object, Text, Text, SpeedBean>{

    SpeedBean speedBean =  new SpeedBean();
    Text k = new Text();

    @SneakyThrows
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        String[] line = value.toString().split(" ");
        String[] line = value.toString().split("\t");
        if (line[1] == null){
            line[1] = "00";
        }
        String carId = line[0]+"_"+line[1]+"_"+line[2];
        k.set(carId);
        speedBean.setCarId(carId);
        speedBean.setDirection(line[3]);
        String str1 = line[3].substring(0,3);
        String str2 = line[3].substring(3,6);
        double km = str1.charAt(0) == 0 ? Double.parseDouble(str1.substring(1,2)):Double.parseDouble(str1);
        double m = str2.charAt(0) == 0 ? Double.parseDouble(str1.substring(1,2)):Double.parseDouble(str2);
        Double position = km+m/1000;
        speedBean.setPosition(position);
        System.out.println();
//        Timestamp time = Timestamp.valueOf(line[5]+" "+line[6]);
        Timestamp time = Timestamp.valueOf(line[5]);
        speedBean.setTime(time.getTime());
        context.write(k, speedBean);
    }
    /**
     *         @Override
     *         public void map(Object key, Text values, Context context) throws IOException,InterruptedException{
     *             StringTokenizer str = new StringTokenizer(values.toString());
     *             while(str.hasMoreTokens()){
     *                 word.set(str.nextToken());
     *                 context.write(word,one);
     *             }
     *         }
     */
}
