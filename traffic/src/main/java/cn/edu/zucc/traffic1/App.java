package cn.edu.zucc.traffic1;

import java.sql.Timestamp;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
/*        String positionStr = "079001";
        double km = positionStr.substring(0,3).charAt(0) == 0  ? Double.parseDouble(positionStr.substring(1,3)):Double.parseDouble(positionStr.substring(0,3));
        double m = positionStr.substring(3,6).charAt(0) == 0 ? Double.parseDouble(positionStr.substring(4,6))/1000:Double.parseDouble(positionStr.substring(3,6))/1000;
        System.out.println(km+"\t"+m);*/
        String str = "2019-08-01 08:04:19";
        String str1 = "2019-08-01 08:05:19";
        long time = Timestamp.valueOf(str).getTime();
        long time1 = Timestamp.valueOf(str1).getTime();
        double speed = (60*60*1000)*1.0/((time1-time));
        System.out.println(speed);
    }
}
