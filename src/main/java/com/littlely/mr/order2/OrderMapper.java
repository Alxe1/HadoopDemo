package com.littlely.mr.order2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    Text k = new Text();
    DoubleWritable v = new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        k.set(fields[0]);
        v.set(Double.parseDouble(fields[2]));

        context.write(k, v);
    }
}
