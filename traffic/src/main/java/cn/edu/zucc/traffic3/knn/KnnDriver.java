package cn.edu.zucc.traffic3.knn;

/**
 * @author: hzhq1255
 * @mail: hzhq1255@163.com
 * @date: 2020/6/14 20:47
 * @desc:
 */
public class KnnDriver {

    public static void main(String[] args) throws Exception {
        String[] task1Paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\output3\\test\\part-r-00000",
                "E:\\program\\Hadoop\\traffic\\output3\\train\\part-r-00000",
                "E:\\program\\Hadoop\\traffic\\output3\\distance"
        };
        String[] task2Paths = new String[]{
                "E:\\program\\Hadoop\\traffic\\output3\\distance\\part-r-00000",
                "E:\\program\\Hadoop\\traffic\\output3\\sortDistance"
        };

        KnnTask1.main(task1Paths);
        for (int k = 10; k < 12; k++){
            KnnTask2.main(task2Paths,k);
            String[] task3Paths = new String[]{
                    "E:\\program\\Hadoop\\traffic\\output3\\sortDistance\\part-r-00000",
                    "E:\\program\\Hadoop\\traffic\\output3\\predict"+k
            };
            KnnTask3.main(task3Paths);
        }

    }
}
