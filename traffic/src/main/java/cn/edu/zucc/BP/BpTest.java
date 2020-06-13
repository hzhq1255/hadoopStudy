package cn.edu.zucc.BP;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/11 12:29
 * @desc:
 */
public class BpTest {

    private int[] wegiht = {0,0,0,0,0,0};


    protected static class Mapper1 extends Mapper<Object,Text,Text,Text>{

    }
}
