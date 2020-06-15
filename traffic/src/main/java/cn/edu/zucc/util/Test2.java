package cn.edu.zucc.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
public class Test2 {



    public static void copyFile() {
        File src = new File("G:\\io\\copysrc.doc");
        File dest = new File("H:\\Hbuilder\\copyesrc.doc");
        // 定义文件输入流和输出流对象
        FileInputStream fis = null;// 输入流
        FileOutputStream fos = null;// 输出流

        try {
            fis = new FileInputStream(src);
            fos = new FileOutputStream(dest);
            byte[] bs = new byte[1024];
            while (true) {
                int len = fis.read(bs, 0, bs.length);
                if (len == -1) {
                    break;
                } else {
                    fos.write(bs, 0, len);
                }
            }
            System.out.println("复制文件成功");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                fis.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String str1 = "1121.0\t87.03\t0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0\t0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0";
        String str2 = "2860.0\t100.26\t0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0\t0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0";
        double distance = StringUtil.getKnnDistance(str1,str2);
        System.out.println(distance);
    }
}
