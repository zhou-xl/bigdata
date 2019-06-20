package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSTest {

    public static void main(String[] args) throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        configuration.set("fs.defaultFS", "hdfs://bigdata130:9000");
        FileSystem fileSystem = FileSystem.get(configuration);

        // 直接配置访问集群的路径和访问集群的用户名称
//		FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata111:9000"),configuration, "itstar");

        // 2 把本地文件上传到文件系统中
        fileSystem.copyFromLocalFile(new Path("C:/Users/zhouxl/Downloads/hello.txt"), new Path("/usr/mod/hello1.copy.txt"));

        // 3 关闭资源
        fileSystem.close();
        System.out.println("over");
    }

}
