package com.littlely.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FileRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream specOut = null;
    FSDataOutputStream otherOut = null;

    public FileRecordWriter(TaskAttemptContext job){

        // 1 获取文件系统
        FileSystem fs;

        try{
            fs = FileSystem.get(job.getConfiguration());
            // 2 创建输出文件路径
            Path specPath = new Path("/test1/spec.log");
            Path otherPath = new Path("/test1/other.log");
            // 3 创建输出流
            specOut = fs.create(specPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        if (key.toString().contains("baidu")){
            specOut.write(key.toString().getBytes());
        }else{
            otherOut.write(key.toString().getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(specOut);
        IOUtils.closeStream(otherOut);
    }
}
