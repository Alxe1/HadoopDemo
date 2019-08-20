package com.littlely.mr.wordcount;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//map阶段
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words){
            k.set(word);
            context.write(k, v);
        }
    }

}
