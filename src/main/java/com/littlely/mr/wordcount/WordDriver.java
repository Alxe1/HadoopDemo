package com.littlely.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.8.100:9000");
        //1获取Job对象
        Job job = Job.getInstance(conf);

        //2设置jar存储位置
        job.setJarByClass(WordDriver.class);

        //3关联Map和Reduce类
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        //4设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7提交job
//		job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0: 1);
    }

}