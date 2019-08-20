package com.littlely.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息及job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 指定jar包所在路径
        job.setJarByClass(FlowCountSortDriver.class);

        // 3 关联mapper和reducer类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 4 指定mapper输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5 指定最终输出kv的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 8 指定自定义数据分区
        job.setPartitionerClass(ProvincePartitioner.class);
        // 9 同时指定相应数量的reduceTask
        job.setNumReduceTasks(5);

        // 6 指定job输入输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数以及job所用的jar包提交yarn
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
