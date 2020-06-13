package cn.edu.zucc.util;

import java.io.*;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/11 21:25
 * @desc:
 */
public class MyFileUtil {

    /**
     * 迭代删除文件
     */
    public static void deleteDir(String dirPath){
        File file = new File(dirPath);
        if (file.exists()){
            if (file.isFile()){
                boolean result = file.delete();
                int tryCount = 0;
                while (!result && tryCount++ < 10) {
                    System.gc(); // 回收资源
                    result = file.delete();
                }
            }
            File[] files = file.listFiles();
            if (files != null)
                for (File value : files) deleteDir(value.getAbsolutePath());
            file.delete();
        }

    }

    public static void copyFile(String src,String dest){
        try {
            FileInputStream fis = new FileInputStream(src);
            FileOutputStream fos = new FileOutputStream(dest);
            byte[] stream = new byte[1024 * 8];
            int len = 0;
            while ((len = fis.read(stream)) != -1) {
                fos.write(stream, 0, len);
            }
            fis.close();
            fos.close();
        }catch (IOException ignored){
            ignored.printStackTrace();
        }
    }

    public static void main(String[] args) {
        copyFile("E:/test/data.txt","E:/test/data1.txt");
    }
}
