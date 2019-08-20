package com.littlely.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置jar存储位置、
        job.setJarByClass(SequenceFileDriver.class);
        // 3 关联mapper和reducer对象
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 8 设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputformat.class);
        // 9 设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 4 设置map输出端的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        // 5 设置最终输出端的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
