package cn.edu.zucc.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/11 22:29
 * @desc:
 */
public class StringUtil {
    /**
     * 欧拉距离
     * @param vector1 向量1
     * @param vector2 向量2
     * @return 两个向量之间的距离
     */
    public static double getDistance(double[] vector1,double[] vector2){
        int len1 = vector1.length;
        int len2 = vector2.length;
        double distance = 0;
        if (len1 == len2){
            for (int i = 0 ; i < len1 ; i++){
                distance += Math.pow(vector1[i]-vector2[i],2.0);
            }
        }
        return distance;
    }

    /**
     * 向量转为字符串
     * @param vector 向量
     * @return 返回字符串
     */
    public static String vectorToString(double[] vector){
        StringBuilder str = new StringBuilder();
        int len = vector.length;
        for (int i = 0; i < len; i++){
            if (i == len - 1){
                str.append(vector[i]);
            }else {
                str.append(vector[i]).append(",");
            }
        }
        return str.toString();
    }

    public static List<double[]> getRandomVector() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("E:/program/Hadoop/traffic/output/car_flow_combine/part-r-00000")));
        String line = "";
        List<double[]> result = new ArrayList<>();
        List<double[]> random = new ArrayList<>();
        while ((line = reader.readLine()) != null){
            String[] value = line.split("\t");
            double[] vector = new double[24];
            for (int i = 0; i < 24; i++){
                vector[i] = Double.parseDouble(value[i+1]);
            }
            result.add(vector);
        }
        int len1 = result.size();
        int count = 0;
        while (count < 3){
            double[] randV = result.get(new Random().nextInt(len1));
            int isExisted = 0;
            for (double[] doubles : random) {
                if (Arrays.equals(doubles, randV)) {
                    isExisted = 1;
                    break;
                }
            }
            if (isExisted == 0){
                random.add(randV);
                count++;
            }
        }
        return random;
    }

    public static String getRandCenterString() throws IOException {
        List<double[]> rands = getRandomVector();
        String center = "";
        int len = rands.size();
        for (int i = 0; i < len; i++){
            if (i == len - 1){
                center+=vectorToString(rands.get(i));
            }else {
                center+=vectorToString(rands.get(i))+"\n";
            }
        }
        return center;
    }

    public static void main(String[] args) throws IOException {
//        List<double[]> test = getRandomVector();
//        for (double[] v:test){
//            System.out.println(vectorToString(v));
//        }
        String center = getRandCenterString();
        System.out.println(center);

    }
}
