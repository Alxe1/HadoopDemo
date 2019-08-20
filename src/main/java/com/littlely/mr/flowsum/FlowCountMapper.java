package com.littlely.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
        //1 获取一行
        String line = value.toString();
        // 2 切割
        String[] fields = line.split("\t");
        //3 对象
        k.set(fields[1]); //封装手机号

        Long upFlow = Long.parseLong(fields[fields.length - 3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        //4 写出
        context.write(k, v);
    }
}
