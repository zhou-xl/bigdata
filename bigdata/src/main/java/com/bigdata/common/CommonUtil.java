package com.bigdata.common;

import java.io.File;

public class CommonUtil {


    /**
     * 删除文件夹
     * @param paths
     */
    public static void deleteFile(String... paths) {
        for (String path : paths) {
            delAllFile(new File(path));
        }
    }

    /**
     * 删除文件
     * @param file
     */
        private static void delAllFile(File file) {
        if (file == null) {
            return;
        } else if (file.exists()) {
            if (file.isFile()) {
                file.delete();
            } else if (file.isDirectory()) {
                String[] str = file.list();
                if (str == null) {
                    file.delete();
                } else {//如果目录不为空，则遍历名字，得到此抽象路径的字符串形式。
                    for (String st : str) {
                        delAllFile(new File(file, st));
                    }//遍历清楚所有当前文件夹里面的所有文件。
                    file.delete();//清楚文件夹里面的东西后再来清楚这个空文件夹
                }
            }
        }
    }

}
