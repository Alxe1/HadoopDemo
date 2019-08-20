package com.littlely.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        //1 获取job对象
        Job job = Job.getInstance(conf);
        //2 设置jar路径
        job.setJarByClass(FlowCountDriver.class);

        //3 关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //4 设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        // 8 指定自定义数据分区
//        job.setPartitionerClass(ProvincePartitioner.class);
//        // 9 同时指定相应数量的reduceTask
//        job.setNumReduceTasks(5);

        //6 设置输出输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);
    }
}
