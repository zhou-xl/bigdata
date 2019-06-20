package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSAPI {

    /**
     * 获取hdfs
     */
    @Test
    public void initHDFS() throws IOException {
        // 创建配置信息
        Configuration config = new Configuration();

        // 获取文件流
        FileSystem fs = FileSystem.get(config);

        // 打印文件系统
        System.out.println(fs.toString());
    }

    /**
     *上传
     */
    @Test
    public void putHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 创建配置信息
        Configuration config = new Configuration();

        // 设置部分的临时参数
        config.set("dfs.replication", "2");

        /* 获取文件系统 */
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata130:9000"), config, "root");

        // windows要上传文件的路劲
        Path input = new Path("C:\\Users\\zhouxl\\Downloads\\hello.txt");

        //输出的路劲 input复制到output
        Path output = new Path("hdfs://bigdata130:9000/plus/idea/word.txt");

        //
        fs.copyFromLocalFile(input, output);

        fs.close();

        System.out.println("上传成功");
    }

    /**
     * 下载
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void getHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 创建配置文件
        Configuration config = new Configuration();

        // 获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata130:9000"), config, "root");

        // 下载文件
        // 1.是不是删除源文件
        // 2.hdfs下载的路劲
        // 3.Windows的路劲
        // 4.是否校验文件
        fs.copyToLocalFile(false, new Path("hdfs://bigdata130:9000/word.txt"),
                new Path("C:\\Users\\zhouxl\\Downloads"), true);

        // 关闭资源
        fs.close();
        System.out.println("下载成功");
    }

    @Test
    public void mkdirHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata130:9000"), conf, "root");

        fs.mkdirs(new Path("hdfs://bigdata130:9000/idea/bb"));

        fs.close();
        System.out.println("创建bb目录");
    }

    /**
     * 获取文件信息
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void deleteHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata130:9000"), conf, "root");


        //
        RemoteIterator<LocatedFileStatus> listFile = fs.listFiles(new Path("hdfs://bigdata130:9000/plus"), true);


/*        while (listFile.hasNext()) {
            LocatedFileStatus next = listFile.next();

            // 文件名字或目录
            System.out.println(next.getPath().getName());
            // 快大小
            System.out.println(next.getBlockSize());
            // 权限
            System.out.println(next.getPermission());

        }*/

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus: fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }
        fs.close();
    }

    /**
     *通过IO上传文件
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void putFileHAFS() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata130:9000"), conf, "root");

        // 创建一个输入流
        FileInputStream fileInputStream = new FileInputStream(new File("C:\\Users\\zhouxl\\Downloads\\hello.txt"));

        // 输出路劲
        Path path = new Path("hdfs://bigdata130:9000/plus/Sogou.txt");

        FSDataOutputStream fsDataOutputStream = fs.create(path);

        // 对接输入输出流

        try {
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4*1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            IOUtils.closeStream(fsDataOutputStream);

            IOUtils.closeStream(fileInputStream);

            fs.close();

            System.out.println("上传成功");

        }

    }

    public void readFileHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata130:9000"), conf, "root");

        Path path = new Path("hdfs://bigdata130:9000");

        FSDataInputStream fis = fs.open(path);

        FileOutputStream fos = new FileOutputStream("C:\\Users\\zhouxl\\Downloads\\a");

        // 定位偏移量offset
        fis.seek(128*1024*1024);



        byte[] bytes = new byte[1024];
        for (int i = 0; i < 128*1024; i++) {
            fis.read();
            fos.write(bytes);
        }

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
