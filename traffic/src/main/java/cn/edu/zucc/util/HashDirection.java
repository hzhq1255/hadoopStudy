package cn.edu.zucc.util;

import java.util.HashMap;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/12 19:15
 * @desc:
 * position = [
 *         '076320',
 *         '079100',
 *         '086060',
 *         '087600',
 *         '092400',
 *         '096350',
 *         '097500',
 *         '103500',
 *         '105710',
 *         '110410',
 *         '115600',
 *         '117210',
 *         '121720',
 *         '126180',
 *         '127700',
 *         '130850',
 *         '135420',
 *         '140900',
 *         '148248',
 *         '151658']
 */
public class HashDirection {
    public static HashMap<String,Integer> capPosition = new HashMap<>();
    static {
        capPosition.put("076320",1);
        capPosition.put("079100",2);
        capPosition.put("086060",3);
        capPosition.put("087600",4);
        capPosition.put("092400",5);
        capPosition.put("096350",6);
        capPosition.put("097500",7);
        capPosition.put("103500",8);
        capPosition.put("105710",9);
        capPosition.put("110410",10);
        capPosition.put("115600",11);
        capPosition.put("117210",12);
        capPosition.put("121720",13);
        capPosition.put("126180",14);
        capPosition.put("127700",15);
        capPosition.put("130850",16);
        capPosition.put("135420",17);
        capPosition.put("140900",18);
        capPosition.put("148248",19);
        capPosition.put("151658",20);
    }

    public static HashMap<Integer,String> codePosition = new HashMap<>();
    static {
        codePosition.put(1,"076320");
        codePosition.put(2,"079100");
        codePosition.put(3,"086060");
        codePosition.put(4,"087600");
        codePosition.put(5,"092400");
        codePosition.put(6,"096350");
        codePosition.put(7,"097500");
        codePosition.put(8,"103500");
        codePosition.put(9,"105710");
        codePosition.put(10,"110410");
        codePosition.put(11,"115600");
        codePosition.put(12,"117210");
        codePosition.put(13,"121720");
        codePosition.put(14,"126180");
        codePosition.put(15,"127700");
        codePosition.put(16,"130850");
        codePosition.put(17,"135420");
        codePosition.put(18,"140900");
        codePosition.put(19,"148248");
        codePosition.put(20,"151658");
    }
}
