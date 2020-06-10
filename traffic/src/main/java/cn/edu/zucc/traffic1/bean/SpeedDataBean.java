package cn.edu.zucc.traffic1.bean;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/9 21:21
 * @desc:
 */
@Data
public class SpeedDataBean {
    private String plate_num;
    private String plate_type;
    private String car_type;
    private String direction;
    private Timestamp time;
}
