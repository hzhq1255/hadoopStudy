package cn.edu.zucc.traffic1.reduce;

import cn.edu.zucc.traffic1.bean.SpeedMap;
import cn.edu.zucc.traffic1.bean.SpeedResult;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/9 21:27
 * @desc:
 */
public class SpeedReducer extends Reducer<Text, SpeedMap,Text, SpeedResult> {

    SpeedResult speedResult = new SpeedResult();
    Double[] positions = new Double[10];
    Long[] times = new Long[10];
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<SpeedMap> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (SpeedMap val:values){
            if (count < 10){
                if (count == 0){
                    speedResult.setPosition1(val.getDirection());
                }
                if (count == 2){
                    speedResult.setPosition2(val.getDirection());
                }
                Timestamp time = new Timestamp(val.getTime());
                String str_time = time.toString();
                speedResult.setHour(str_time.substring(11,13));
                positions[count] = val.getPosition();
                times[count] = val.getTime();
            }
            count++;
        }
        Double distance = positions[0] > positions[1] ? positions[0] - positions[1]: positions[1]- positions[0];
        Long spendTime = times[0] > times[1] ? times[0] - times[1] : times[1] - times[0];
        Double spend = (distance/spendTime)*(60*60*1000);
        spend = Double.parseDouble(String.format("%.2f",spend));
        speedResult.setSpeed(spend);
        if (spend >= 60.0 && spend <= 120.0){
            k.set(key);
            context.write(k,speedResult);
        }
    }
}
