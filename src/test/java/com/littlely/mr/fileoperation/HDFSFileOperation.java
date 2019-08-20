package com.littlely.mr.fileoperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSFileOperation {

    // 本地文件拷贝到HDFS
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos03:9000"), conf, "root");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("F:"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/dd.txt"));
        // 4. 流的对拷
        IOUtils.copyBytes(fis, fos, conf);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 从HDFS上下载文件
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://centos03:9000"), conf, "root");

        FSDataInputStream fis = fs.open(new Path("/dd.txt"));
        FileOutputStream fos = new FileOutputStream(new File("F:/ds.txt"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}


